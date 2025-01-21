package catchup

import (
	"context"
	"sync"
	"time"

	"github.com/Conflux-Chain/confura/store"
	logutil "github.com/Conflux-Chain/go-conflux-util/log"
	"github.com/sirupsen/logrus"
)

type worker struct {
	// worker name
	name string
	// result channel to collect queried epoch data
	resultChan chan *store.EpochData
	// RPC client delegated to fetch blockchain data
	client AbstractRpcClient
}

func mustNewWorker(name string, client AbstractRpcClient, chanSize int) *worker {
	return &worker{
		name:       name,
		resultChan: make(chan *store.EpochData, chanSize),
		client:     client,
	}
}

func (w *worker) Sync(ctx context.Context, wg *sync.WaitGroup, epochFrom, epochTo, stepN uint64) {
	defer wg.Done()

	etLogger := logutil.NewErrorTolerantLogger(logutil.DefaultETConfig)
	for eno := epochFrom; eno <= epochTo; {
		select {
		case <-ctx.Done():
			return
		default:
			epochData, err := w.fetchEpoch(eno)
			etLogger.Log(
				logrus.WithFields(logrus.Fields{
					"epochNo":    eno,
					"workerName": w.name,
				}), err, "Catch-up worker failed to fetch epoch",
			)

			if err != nil {
				time.Sleep(time.Second)
				break
			}

			select {
			case <-ctx.Done():
				return
			case w.resultChan <- epochData:
				eno += stepN
			}
		}
	}
}

func (w *worker) Close() {
	w.client.Close()
	close(w.resultChan)
}

func (w *worker) Data() <-chan *store.EpochData {
	return w.resultChan
}

func (w *worker) fetchEpoch(epochNo uint64) (*store.EpochData, error) {
	epochData, err := w.client.QueryEpochData(context.Background(), epochNo, epochNo)
	if err == nil {
		return epochData[0], nil
	}

	return nil, err
}
