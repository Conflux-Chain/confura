package mysql

import (
	"sync"
	"testing"

	"github.com/Conflux-Chain/confura/store"
	"github.com/stretchr/testify/require"
)

func TestPrunerConcurrentStartAndClose(t *testing.T) {
	sp := newStorePruner(policyTestDB(t, false))
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); sp.schedulePrune(&Config{}); sp.Close() }()
	}
	wg.Wait()
	require.Error(t, sp.ctx.Err())
	// Producers may still hold the notification channel. It remains open, and
	// the cancellation branch lets them leave after the observer has stopped.
	for i := 0; i < 10; i++ {
		select {
		case <-sp.ctx.Done():
		case sp.newBnPartitionObsChan <- &bnPartition{}:
		}
	}
	sp.Close()
}

func TestMySQLCloneDoesNotOwnPruner(t *testing.T) {
	db := policyTestDB(t, true)
	ms := NewMysqlStore[*store.EthData](db, &Config{}, nil)
	clone, err := ms.Clone()
	require.NoError(t, err)
	ms.Prune()
	require.NoError(t, clone.Close())
	require.NoError(t, clone.Close())
	require.NoError(t, ms.pruner.ctx.Err())
	require.NoError(t, ms.Close())
	require.NoError(t, ms.Close())
	require.Error(t, ms.pruner.ctx.Err())
	ms.pruner.wg.Wait()
}
