package catchup

import (
	"context"
	"sync"
	"time"

	"github.com/Conflux-Chain/confura/store"
	"github.com/Conflux-Chain/confura/util/rpc"
	sdk "github.com/Conflux-Chain/go-conflux-sdk"
	logutil "github.com/Conflux-Chain/go-conflux-util/log"
	"github.com/sirupsen/logrus"
)

type worker struct {
	// worker name
	name string
	// result channel to collect queried epoch data
	resultChan chan *store.EpochData
	// conflux sdk client delegated to fetch epoch data
	cfx sdk.ClientOperator
}

func mustNewWorker(name, nodeUrl string, chanSize int) *worker {
	return &worker{
		name:       name,
		resultChan: make(chan *store.EpochData, chanSize),
		cfx:        rpc.MustNewCfxClient(nodeUrl),
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
	w.cfx.Close()
	close(w.resultChan)
}

func (w *worker) Data() <-chan *store.EpochData {
	return w.resultChan
}

func (w *worker) fetchEpoch(epochNo uint64) (*store.EpochData, error) {
	epochData, err := store.QueryEpochData(w.cfx, epochNo, true)
	if err == nil {
		return &epochData, nil
	}

	return nil, err
}
