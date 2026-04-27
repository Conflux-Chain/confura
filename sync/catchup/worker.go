package catchup

import (
	"context"
	"sync"
	"time"

	"github.com/Conflux-Chain/confura/store"
	"github.com/sirupsen/logrus"
)

type worker[T store.ChainData] struct {
	// worker name
	name string
	// result channel to collect queried epoch data
	resultChan chan T
	// RPC client delegated to fetch blockchain data
	client IRpcClient[T]
}

func mustNewWorker[T store.ChainData](name string, client IRpcClient[T], chanSize int) *worker[T] {
	return &worker[T]{
		name:       name,
		resultChan: make(chan T, chanSize),
		client:     client,
	}
}

func (w *worker[T]) Sync(ctx context.Context, wg *sync.WaitGroup, epochFrom, epochTo, stepN uint64) {
	defer wg.Done()

	for eno := epochFrom; eno <= epochTo; {
		select {
		case <-ctx.Done():
			return
		default:
			data, err := w.client.QueryChainData(ctx, eno, eno)
			if err != nil {
				logrus.WithFields(logrus.Fields{
					"epochNo":    eno,
					"workerName": w.name,
				}).WithError(err).Info("Catch-up worker failed to fetch epoch")
				time.Sleep(time.Second)
				break
			}

			select {
			case <-ctx.Done():
				return
			case w.resultChan <- data[0]:
				eno += stepN
			}
		}
	}
}

func (w *worker[T]) Close() {
	w.client.Close()
	close(w.resultChan)
}

func (w *worker[T]) Data() <-chan T {
	return w.resultChan
}
