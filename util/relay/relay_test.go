package relay

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestRelayConcurrentProducersNeverWaitForConsumer(t *testing.T) {
	r := newTxnRelayer(&TxnRelayerConfig{BufferSize: 2})
	raw := []byte{1, 2}
	if !r.Relay(raw) {
		t.Fatal("initial enqueue failed")
	}
	raw[0] = 9
	var accepted atomic.Int32
	var wg sync.WaitGroup
	start, done := make(chan struct{}), make(chan struct{})
	for i := 0; i < 64; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			if r.Relay([]byte{3}) {
				accepted.Add(1)
			}
		}()
	}
	close(start)
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("producer blocked without consumer")
	}
	if accepted.Load() != 1 {
		t.Fatalf("accepted %d beyond remaining capacity", accepted.Load())
	}
	if (<-r.txnQueue)[0] != 1 {
		t.Fatal("caller mutated queued bytes")
	}
}
