//go:build integration

package sync

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	gosync "sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Conflux-Chain/confura/store/mysql"
	sdk "github.com/Conflux-Chain/go-conflux-sdk"
	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/require"
)

func TestTraceAdapterLivesThroughLatestSync(t *testing.T) {
	db, cfg := newSyncTestDB(t)
	require.NoError(t, db.AutoMigrate(&mysql.CfxTraceSyncEpochBlockMap{}))
	ms := mysql.NewCfxStore(db, cfg, syncTestFilter{})
	defer ms.Close()
	latest, release := make(chan struct{}), make(chan struct{})
	var seen, unblock gosync.Once
	releaseLatest := func() { unblock.Do(func() { close(release) }) }
	defer releaseLatest()
	var active atomic.Int32
	block := func(n uint64) map[string]interface{} {
		hash := func(n uint64) string { return fmt.Sprintf("0x%064x", n+1) }
		parent := uint64(0)
		if n > 0 {
			parent = n - 1
		}
		return map[string]interface{}{"hash": hash(n), "parentHash": hash(parent), "epochNumber": fmt.Sprintf("0x%x", n), "blockNumber": fmt.Sprintf("0x%x", n), "height": fmt.Sprintf("0x%x", n), "transactions": []interface{}{}, "gasUsed": "0x0", "gasLimit": "0x1"}
	}
	upgrader := websocket.Upgrader{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		active.Add(1)
		defer active.Add(-1)
		defer conn.Close()
		for {
			var req struct {
				ID     json.RawMessage   `json:"id"`
				Method string            `json:"method"`
				Params []json.RawMessage `json:"params"`
			}
			if conn.ReadJSON(&req) != nil {
				return
			}
			var result interface{}
			switch req.Method {
			case "cfx_getStatus":
				result = map[string]interface{}{"networkId": "0x1", "chainId": "0x1"}
			case "cfx_epochNumber":
				var tag string
				_ = json.Unmarshal(req.Params[0], &tag)
				result = "0x0"
				if tag == "latest_state" {
					seen.Do(func() { close(latest) })
					<-release
					result = "0x2"
				}
			case "cfx_getBlockByEpochNumber":
				result = block(0)
			case "cfx_getBlocksByEpoch":
				var epoch string
				_ = json.Unmarshal(req.Params[0], &epoch)
				n, _ := strconv.ParseUint(strings.TrimPrefix(epoch, "0x"), 16, 64)
				result = []string{fmt.Sprintf("0x%064x", n+1)}
			case "cfx_getBlockByHashWithPivotAssumption":
				var epoch string
				_ = json.Unmarshal(req.Params[2], &epoch)
				n, _ := strconv.ParseUint(strings.TrimPrefix(epoch, "0x"), 16, 64)
				result = block(n)
			default:
				t.Errorf("unexpected RPC %s", req.Method)
				return
			}
			if conn.WriteJSON(map[string]interface{}{"jsonrpc": "2.0", "id": req.ID, "result": result}) != nil {
				return
			}
		}
	}))
	defer server.Close()
	client, err := sdk.NewClient("ws" + strings.TrimPrefix(server.URL, "http"))
	require.NoError(t, err)
	defer client.Close()
	syncer, err := NewTraceLogSyncer([]*sdk.Client{client}, ms)
	require.NoError(t, err)
	syncer.conf.Poller.IdleInterval = 10 * time.Millisecond
	syncer.conf.Poller.RetryInterval = 10 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	var wg gosync.WaitGroup
	returned := make(chan error, 1)
	go func() { returned <- syncer.Sync(ctx, &wg) }()
	defer func() {
		cancel()
		releaseLatest()
		done := make(chan struct{})
		go func() { wg.Wait(); close(done) }()
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Error("trace workers did not stop")
		}
	}()
	select {
	case <-latest:
	case err := <-returned:
		t.Fatalf("Sync returned before live polling: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("trace did not reach live polling")
	}
	select {
	case err := <-returned:
		t.Fatalf("Sync returned before live workers stopped: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	require.Equal(t, int32(2), active.Load(), "polling adapter must remain connected")
	releaseLatest()
	require.Eventually(t, func() bool {
		epoch, ok, err := syncer.epochBlockMapStore.MaxEpoch()
		return err == nil && ok && epoch == 2
	}, 5*time.Second, 10*time.Millisecond, "live epoch did not commit")
	cancel()
	select {
	case err := <-returned:
		require.NoError(t, err)
	case <-time.After(3 * time.Second):
		t.Fatal("Sync did not return after cancellation")
	}
	require.Eventually(t, func() bool { return active.Load() == 1 }, time.Second, 10*time.Millisecond, "owned adapter was not closed")
	var count int64
	require.NoError(t, db.Model(&mysql.CfxTraceSyncEpochBlockMap{}).Count(&count).Error)
	require.Equal(t, int64(3), count, "catch-up and live epochs must each commit once")
}
