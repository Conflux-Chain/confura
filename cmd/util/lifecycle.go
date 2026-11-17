package util

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

// Lifecycle installs signal handling before startup. Shutdown joins all users
// before their dependencies are released. Close is called after that cleanup.
type Lifecycle struct {
	errMu    sync.Mutex
	err      error
	Ctx      context.Context
	Wg       sync.WaitGroup
	cancel   context.CancelFunc
	signals  chan os.Signal
	finished chan struct{}
	done     chan struct{}
	once     sync.Once
}

func NewLifecycle(parent context.Context) (*Lifecycle, error) {
	timeout := 30 * time.Second
	if viper.IsSet("shutdown.timeout") {
		timeout = viper.GetDuration("shutdown.timeout")
	}
	if timeout <= 0 {
		return nil, fmt.Errorf("shutdown.timeout must be positive")
	}
	ctx, cancel := context.WithCancel(parent)
	l := &Lifecycle{Ctx: ctx, cancel: cancel, signals: make(chan os.Signal, 2), finished: make(chan struct{}), done: make(chan struct{})}
	signal.Notify(l.signals, syscall.SIGTERM, syscall.SIGINT)
	go l.watch(timeout)
	return l, nil
}

func (l *Lifecycle) watch(timeout time.Duration) {
	defer close(l.done)
	select {
	case <-l.signals:
		l.cancel()
	case <-l.Ctx.Done():
	case <-l.finished:
		return
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-l.finished:
		return
	case <-l.signals:
		logrus.Error("Second termination signal: forcing shutdown")
	case <-timer.C:
		logrus.Error("Shutdown deadline exceeded: in-flight writes may have an uncertain outcome")
	}
	// Do not close databases while writers may still be running.
	os.Exit(1)
}

func (l *Lifecycle) Shutdown() { l.cancel(); l.Wg.Wait() }
func (l *Lifecycle) Wait()     { <-l.Ctx.Done() }
func (l *Lifecycle) Close() {
	l.once.Do(func() { close(l.finished); signal.Stop(l.signals); <-l.done; l.cancel() })
}

// RunTracked registers the enclosing task before it can start or Wait can run.
// A private group also joins tasks registered by legacy blocking Run methods.
func RunTracked(ctx context.Context, wg *sync.WaitGroup, run func(context.Context, *sync.WaitGroup)) {
	wg.Add(1)
	go func() { defer wg.Done(); var inner sync.WaitGroup; run(ctx, &inner); inner.Wait() }()
}

func (l *Lifecycle) Fail(err error) {
	if err == nil {
		return
	}
	l.errMu.Lock()
	if l.err == nil {
		l.err = err
	}
	l.errMu.Unlock()
	l.cancel()
}
func (l *Lifecycle) Err() error { l.errMu.Lock(); defer l.errMu.Unlock(); return l.err }
