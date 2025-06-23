package cache

import (
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/openweb3/web3go/types"
	"github.com/stretchr/testify/assert"
)

func mockTransaction(hash string) *types.TransactionDetail {
	return &types.TransactionDetail{
		Hash: common.HexToHash(hash),
	}
}

func TestEthCachePendingTransaction(t *testing.T) {
	t.Parallel()

	t.Run("Add and Get PendingTransaction", func(t *testing.T) {
		ethCache := newEthCache(newEthCacheConfig())
		tx := mockTransaction("0xaaa")
		ethCache.AddPendingTransaction(tx)

		pending, loaded, expired, err := ethCache.GetPendingTransaction(tx.Hash)
		assert.NoError(t, err)
		assert.True(t, loaded)
		assert.False(t, expired)
		assert.Equal(t, tx, pending.TransactionDetail)
	})

	t.Run("Duplicate Add Should Not Overwrite", func(t *testing.T) {
		ethCache := newEthCache(newEthCacheConfig())
		tx := mockTransaction("0xbbb")
		ethCache.AddPendingTransaction(tx)

		first, _, _, _ := ethCache.GetPendingTransaction(tx.Hash)

		// Try add again
		ethCache.AddPendingTransaction(tx)

		second, _, _, _ := ethCache.GetPendingTransaction(tx.Hash)
		assert.Equal(t, first, second)
	})

	t.Run("Remove PendingTransaction", func(t *testing.T) {
		ethCache := newEthCache(newEthCacheConfig())
		tx := mockTransaction("0xccc")
		ethCache.AddPendingTransaction(tx)

		removed := ethCache.RemovePendingTransaction(tx.Hash)
		assert.True(t, removed)

		_, loaded, _, _ := ethCache.GetPendingTransaction(tx.Hash)
		assert.False(t, loaded)
	})

	t.Run("ShouldCheckNow expiration logic", func(t *testing.T) {
		// override config for faster test
		cfg := newEthCacheConfig()
		cfg.PendingTxnCheckExemption = 1 * time.Second
		cfg.PendingTxnCheckInterval = 500 * time.Millisecond
		ethCache := newEthCache(cfg)

		tx := mockTransaction("0xddd")
		ethCache.AddPendingTransaction(tx)

		time.Sleep(1100 * time.Millisecond)

		pending, loaded, expired, err := ethCache.GetPendingTransaction(tx.Hash)
		assert.NoError(t, err)
		assert.True(t, loaded)
		assert.True(t, expired, "should expire after exemption")

		pending.MarkChecked()
		_, _, expired, _ = ethCache.GetPendingTransaction(tx.Hash)
		assert.False(t, expired, "just checked, should not expire")

		time.Sleep(cfg.PendingTxnCheckInterval + 100*time.Millisecond)
		_, _, expired, _ = ethCache.GetPendingTransaction(tx.Hash)
		assert.True(t, expired, "should expire after interval")
	})

	t.Run("MarkChecked updates timestamp", func(t *testing.T) {
		tx := mockTransaction("0xeee")
		ptx := &ethPendingTxn{
			TransactionDetail: tx,
			createdAt:         time.Now().Add(-5 * time.Minute),
		}

		exempt := 1 * time.Minute
		interval := 1 * time.Second

		assert.True(t, ptx.shouldCheckNow(exempt, interval), "should check when never checked")

		ptx.MarkChecked()
		assert.False(t, ptx.shouldCheckNow(exempt, interval), "just checked")
	})
}
