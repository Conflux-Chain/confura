package node

import (
	"context"
	"math/rand"
	"strconv"
	"testing"
	"time"
)

var (
	groupNodeUrls = map[Group][]string{
		"cfxfilter": {
			"http://127.0.0.1:25372",
		},
		"cfxhttp": {
			"http://127.0.0.1:25373",
			"http://127.0.0.1:25374",
			"http://127.0.0.1:25375",
		},
		"cfxlog": {
			"http://127.0.0.1:25372",
		},
		"cfxws": {
			"ws://127.0.0.1:25351",
			"ws://127.0.0.1:25352",
			"ws://127.0.0.1:25353",
		},
	}
)

type dummyNode struct {
	*baseNode
}

func newDummyNode(group Group, name, url string, hm HealthMonitor) (*dummyNode, error) {
	_, cancel := context.WithCancel(context.Background())
	n := &dummyNode{newBaseNode(name, url, cancel)}

	n.atomicStatus.Store(NewStatus(group, name))
	return n, nil
}

func (n *dummyNode) LatestEpochNumber() (uint64, error) {
	panic("not supported")
}

func BenchmarkNodePoolRoute(b *testing.B) {
	pool := newNodePool(func(group Group, name, url string, hm HealthMonitor) (Node, error) {
		return newDummyNode(group, name, url, hm)
	})

	var groups []Group
	for grp, urls := range groupNodeUrls {
		pool.add(grp, urls...)
		groups = append(groups, grp)
	}

	rnd := rand.New(rand.NewSource(time.Now().UnixNano()))

	b.ResetTimer()

	// run the `manager.Route` function b.N times
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		grp := groups[rnd.Intn(len(groups))]
		key := strconv.Itoa(rnd.Intn(b.N))
		b.StartTimer()

		mgr, ok := pool.manager(grp)
		if !ok {
			b.Fatal("group not registered")
		}

		url := mgr.Route([]byte(key))
		if len(url) == 0 {
			b.Fatal("route empty url")
		}
	}
}
