package relay

import (
	"fmt"
	"runtime"
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

func TestRelayFullQueueDoesNotCopy(t *testing.T) {
	r := newTxnRelayer(&TxnRelayerConfig{BufferSize: 1})
	if !r.Relay([]byte{1}) {
		t.Fatal("initial admission failed")
	}
	raw := make([]byte, 128<<10)
	if allocs := testing.AllocsPerRun(100, func() {
		if r.Relay(raw) {
			t.Fatal("accepted into a full queue")
		}
	}); allocs != 0 {
		t.Fatalf("full queue rejection allocated %v times", allocs)
	}
}

func TestRelayUnbufferedAdmission(t *testing.T) {
	r := newTxnRelayer(&TxnRelayerConfig{BufferSize: 0})
	if r.Relay([]byte{1}) {
		t.Fatal("unbuffered queue accepted without a receiver")
	}
	received := make(chan []byte, 1)
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		select {
		case raw := <-r.txnQueue:
			received <- raw
		case <-stop:
		}
	}()
	defer func() { close(stop); <-done }()
	raw := []byte{2}
	deadline := time.Now().Add(time.Second)
	for !r.Relay(raw) {
		if time.Now().After(deadline) {
			t.Fatal("unbuffered queue never admitted with a waiting receiver")
		}
		runtime.Gosched()
	}
	raw[0] = 9
	if got := <-received; got[0] != 2 {
		t.Fatal("caller mutated unbuffered transaction bytes")
	}
}

func BenchmarkRelayAdmission(b *testing.B) {
	for _, full := range []bool{false, true} {
		for _, size := range []int{32, 128 << 10} {
			b.Run(fmt.Sprintf("full=%t/%d", full, size), func(b *testing.B) {
				r := newTxnRelayer(&TxnRelayerConfig{BufferSize: 1})
				raw := make([]byte, size)
				if full && !r.Relay(raw) {
					b.Fatal("initial admission failed")
				}
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if r.Relay(raw) == full {
						b.Fatal("unexpected admission result")
					}
					if !full {
						<-r.txnQueue
					}
				}
			})
		}
	}
}
