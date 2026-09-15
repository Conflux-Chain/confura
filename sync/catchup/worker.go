package catchup

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/Conflux-Chain/confura/store"
	"github.com/sirupsen/logrus"
)

type sourcedChainData[T store.ChainData] struct {
	data     T
	sourceID string
}

type worker[T store.ChainData] struct {
	// worker name
	name string
	// stable source of the RPC client delegated to this worker
	sourceID string
	// result channel to collect queried epoch data
	resultChan chan sourcedChainData[T]
	// RPC client delegated to fetch blockchain data
	client IRpcClient[T]
}

func mustNewWorker[T store.ChainData](name string, client IRpcClient[T], chanSize int) *worker[T] {
	sourceID := client.SourceID()
	if sourceID == "" {
		sourceID = fmt.Sprintf("%s:%s", client.Space(), name)
	}

	return &worker[T]{
		name:       name,
		sourceID:   sourceID,
		resultChan: make(chan sourcedChainData[T], chanSize),
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
			case w.resultChan <- sourcedChainData[T]{data: data[0], sourceID: w.sourceID}:
				eno += stepN
			}
		}
	}
}

func (w *worker[T]) Close() {
	w.client.Close()
	close(w.resultChan)
}

func (w *worker[T]) Data() <-chan sourcedChainData[T] {
	return w.resultChan
}
