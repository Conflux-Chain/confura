package node

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLocalSnapshotRemovesAbsentGroups(t *testing.T) {
	setupNodeTest(t)
	initial := map[Group][]string{GroupEthHttp: {"http://127.0.0.1:19001"}, GroupEthLogs: {"http://127.0.0.1:19002"}}
	local := NewLocalRouter(initial)
	local.update(map[Group][]string{GroupEthLogs: {"http://127.0.0.1:19003"}})
	url, err := local.Route(context.Background(), GroupEthHttp, nil)
	require.NoError(t, err)
	require.Empty(t, url)
	require.Empty(t, local.ListNodesByGroup(GroupEthHttp))
	require.Equal(t, []string{"http://127.0.0.1:19003"}, local.ListNodesByGroup(GroupEthLogs))
	local.update(map[Group][]string{GroupEthLogs: nil})
	require.Empty(t, local.ListNodesByGroup(GroupEthLogs))
	local.update(initial)
	local.update(initial)
	require.Equal(t, []string{"http://127.0.0.1:19001"}, local.ListNodesByGroup(GroupEthHttp))
	local.update(nil)
	require.Empty(t, local.groups)
}

func TestLocalSnapshotPublishesCompleteGroups(t *testing.T) {
	setupNodeTest(t)
	a := map[Group][]string{GroupEthHttp: {"http://127.0.0.1:19001"}, GroupEthLogs: {"http://127.0.0.1:19002"}}
	b := map[Group][]string{GroupEthWs: {"ws://127.0.0.1:19003"}}
	local := NewLocalRouter(a)
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				local.mu.Lock()
				if len(local.groups) == 2 {
					if local.groups[GroupEthHttp] == nil || local.groups[GroupEthLogs] == nil {
						t.Error("partial old snapshot")
					}
				} else if len(local.groups) == 1 {
					if local.groups[GroupEthWs] == nil {
						t.Error("partial new snapshot")
					}
				} else {
					t.Error("mixed group sets")
				}
				local.mu.Unlock()
				_, err := local.Route(context.Background(), GroupEthHttp, []byte("key"))
				if err != nil {
					t.Error(err)
				}
				local.ListNodesByGroup(GroupEthLogs)
			}
		}()
	}
	for i := 0; i < 50; i++ {
		local.update(b)
		local.update(a)
	}
	wg.Wait()
}
