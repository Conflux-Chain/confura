package node

import (
	"errors"
	"sync"
	"testing"
	"time"

	metricutil "github.com/Conflux-Chain/go-conflux-util/metrics"
	"github.com/stretchr/testify/require"
)

func TestHealthReferenceDropsRemovedMembers(t *testing.T) {
	setupNodeTest(t)
	manager := NewManager(GroupEthHttp)
	high := newTestNode(GroupEthHttp, "high", "http://127.0.0.1:19001")
	low := newTestNode(GroupEthHttp, "low", "http://127.0.0.1:19002")
	defer high.Close()
	defer low.Close()
	high.epoch = 1000
	manager.Add(high, low)
	hs, ls := high.Status(), low.Status()
	hs.Update(high)
	hs.Report(manager)
	ls.Update(low)
	ls.Report(manager)
	unhealthy, _ := manager.HealthStatus(low.Name())
	require.True(t, unhealthy)
	require.EqualValues(t, 1000, manager.HealthyEpoch())
	for i := 0; i < 5; i++ {
		ls.Update(low)
		ls.Report(manager)
	}
	unhealthy, _ = manager.HealthStatus(low.Name())
	require.True(t, unhealthy, "the current high node must still constrain recovery")
	manager.Remove(high.Name())
	require.Zero(t, manager.HealthyEpoch(), "the remaining unhealthy node has not reported a valid epoch")
	ls.successCounter = 0
	for i := 0; i < 2; i++ {
		ls.Update(low)
		ls.Report(manager)
	}
	require.Empty(t, manager.Route(nil))
	ls.Update(low)
	ls.Report(manager)
	require.Equal(t, low.Url(), manager.Route(nil))
}

func TestHealthReferenceRecomputesMedianAndResetsEmptyGroup(t *testing.T) {
	setupNodeTest(t)
	manager := NewManager(GroupEthHttp)
	a, b, c := newTestNode(GroupEthHttp, "a", "a"), newTestNode(GroupEthHttp, "b", "b"), newTestNode(GroupEthHttp, "c", "c")
	defer a.Close()
	defer b.Close()
	defer c.Close()
	manager.Add(a, b, c)
	manager.ReportEpoch("a", 10)
	manager.ReportEpoch("b", 20)
	manager.ReportEpoch("c", 30)
	require.EqualValues(t, 20, manager.HealthyEpoch())
	manager.Remove("b")
	require.EqualValues(t, 30, manager.HealthyEpoch())
	manager.Remove("c")
	require.EqualValues(t, 10, manager.HealthyEpoch())
	manager.Remove("a")
	require.Zero(t, manager.HealthyEpoch())
	manager.ReportEpoch("b", 1000)
	require.Empty(t, manager.monitorStatuses)
	manager.Add(a)
	require.Zero(t, manager.HealthyEpoch())
	manager.ReportEpoch("a", 7)
	require.EqualValues(t, 7, manager.HealthyEpoch())
}

func TestHealthReferenceConcurrentMembershipChanges(t *testing.T) {
	setupNodeTest(t)
	manager := NewManager(GroupEthHttp)
	n := newTestNode(GroupEthHttp, "changing", "http://127.0.0.1:19001")
	defer n.Close()
	manager.Add(n)
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				manager.ReportEpoch(n.Name(), uint64(j))
				manager.ReportHealthy(n.Name())
			}
		}()
	}
	for i := 0; i < 25; i++ {
		manager.Remove(n.Name())
		manager.Add(n)
	}
	wg.Wait()
	manager.Remove(n.Name())
	require.Zero(t, manager.HealthyEpoch())
	require.Empty(t, manager.monitorStatuses)
	require.Empty(t, manager.Route(nil))
}

func TestHealthRecoveryStillRequiresConsecutiveSuccesses(t *testing.T) {
	setupNodeTest(t)
	manager := NewManager(GroupEthHttp)
	n := newTestNode(GroupEthHttp, "flapping", "http://127.0.0.1:19001")
	defer n.Close()
	manager.Add(n)
	s := n.Status()
	s.Update(n)
	s.Report(manager)
	n.err = errors.New("offline")
	for i := 0; i < 3; i++ {
		s.Update(n)
		s.Report(manager)
	}
	require.Empty(t, manager.Route(nil))
	n.err = nil
	for i := 0; i < 2; i++ {
		s.Update(n)
		s.Report(manager)
	}
	require.Empty(t, manager.Route(nil))
	n.err = errors.New("flap")
	s.Update(n)
	s.Report(manager)
	require.Zero(t, s.successCounter)
	n.err = nil
	for i := 0; i < 3; i++ {
		s.Update(n)
		s.Report(manager)
	}
	require.Equal(t, n.Url(), manager.Route(nil))
	histogram := metricutil.GetOrRegisterHistogram("%s", s.metric.latency)
	histogram.Clear()
	histogram.Update(int64(4 * time.Second))
	s.Report(manager)
	require.Empty(t, manager.Route(nil), "slow nodes must still fail the latency check")
}
