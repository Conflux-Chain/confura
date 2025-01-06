package util

import (
	"context"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/sirupsen/logrus"
)

// GracefulShutdownContext wraps context and wait group which can be utilized
// to handle graceful shutdown on termination signal received.
type GracefulShutdownContext struct {
	Ctx context.Context
	Wg  *sync.WaitGroup
}

// GracefulShutdown supports to clean up goroutines after termination signal captured.
func GracefulShutdown(wg *sync.WaitGroup, cancel context.CancelFunc) {
	// Handle sigterm and await termChan signal
	termChan := make(chan os.Signal, 1)
	signal.Notify(termChan, syscall.SIGTERM, syscall.SIGINT)

	// Wait for SIGTERM to be captured
	<-termChan
	logrus.Info("SIGTERM/SIGINT received, shutdown process initiated")

	// Cancel to notify active goroutines to clean up.
	cancel()

	logrus.Info("Waiting for shutdown...")
	wg.Wait()

	logrus.Info("Shutdown gracefully")
	logrus.Fatal("Bye!")
}

// StartAndGracefulShutdown starts to run the specified task in a goroutine and wait for termination
// signal to shutdown gracefully.
//
// Note, this method is not suitable for any non-blocking task that release resources in defer way.
func StartAndGracefulShutdown(run func(ctx context.Context, wg *sync.WaitGroup)) {
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup

	if run != nil {
		go run(ctx, &wg)
	}

	GracefulShutdown(&wg, cancel)
}
