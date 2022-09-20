package catchup

import (
	"context"
	"sync"
	"time"

	sdk "github.com/Conflux-Chain/go-conflux-sdk"
	"github.com/scroll-tech/rpc-gateway/store"
	"github.com/scroll-tech/rpc-gateway/util/rpc"
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

	for eno := epochFrom; eno <= epochTo; {
		epochData, ok := w.fetchEpoch(ctx, eno)
		if !ok {
			return
		}

		select {
		case <-ctx.Done():
			return
		default:
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

func (w *worker) fetchEpoch(ctx context.Context, epochNo uint64) (*store.EpochData, bool) {
	for try := 1; ; try++ {
		select {
		case <-ctx.Done():
			return nil, false
		default:
		}

		epochData, err := store.QueryEpochData(w.cfx, epochNo, true)
		if err == nil {
			return &epochData, true
		}

		logger := logrus.WithFields(logrus.Fields{
			"epochNo": epochNo, "workerName": w.name,
		}).WithError(err)

		logf := logger.Debug
		if try%50 == 0 {
			logf = logger.Error
		}

		logf("Catch-up worker failed to fetch epoch")
		time.Sleep(time.Second)
	}
}
