package test

import (
	"context"
	"sync"
	"time"
)

// validatorTasks owns subscriptions, pollers and comparison loops. Stop prevents
// new registration before waiting, so concurrent Destroy cannot race with Add.
type validatorTasks struct {
	ctx         context.Context
	cancel      context.CancelFunc
	mu          sync.Mutex
	stopped     bool
	wg          sync.WaitGroup
	destroyOnce sync.Once
}

func newValidatorTasks() *validatorTasks {
	ctx, cancel := context.WithCancel(context.Background())
	return &validatorTasks{ctx: ctx, cancel: cancel}
}

func (v *validatorTasks) start(run func()) {
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.stopped {
		return
	}
	v.wg.Add(1)
	go func() { defer v.wg.Done(); run() }()
}

func (v *validatorTasks) stop() {
	v.mu.Lock()
	v.stopped = true
	v.cancel()
	v.mu.Unlock()
	v.wg.Wait()
}

func (v *validatorTasks) waitRetry(delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-v.ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func (v *validatorTasks) run(ctx context.Context, wg *sync.WaitGroup, setup func()) {
	wg.Add(1)
	defer wg.Done()
	stop := context.AfterFunc(ctx, v.cancel)
	defer stop()
	setup()
	<-v.ctx.Done()
	v.stop()
}
