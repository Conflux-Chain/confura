package cache

import (
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/stretchr/testify/assert"
)

func TestEthCachePendingTransaction(t *testing.T) {
	t.Parallel()

	t.Run("Add and Get PendingTransaction", func(t *testing.T) {
		ethCache := newEthCache(newEthCacheConfig())
		txHash := common.HexToHash("0xaaa")
		ethCache.AddPendingTransaction(txHash)

		pending, loaded, expired := ethCache.GetPendingTransaction(txHash)
		assert.True(t, loaded)
		assert.False(t, expired)
		assert.NotNil(t, pending)
	})

	t.Run("Duplicate Add Should Not Overwrite", func(t *testing.T) {
		ethCache := newEthCache(newEthCacheConfig())
		txHash := common.HexToHash("0xbbb")
		ethCache.AddPendingTransaction(txHash)

		first, _, _ := ethCache.GetPendingTransaction(txHash)

		// Try add again
		ethCache.AddPendingTransaction(txHash)

		second, _, _ := ethCache.GetPendingTransaction(txHash)
		assert.Equal(t, first, second)
	})

	t.Run("Remove PendingTransaction", func(t *testing.T) {
		ethCache := newEthCache(newEthCacheConfig())
		txHash := common.HexToHash("0xccc")
		ethCache.AddPendingTransaction(txHash)

		removed := ethCache.RemovePendingTransaction(txHash)
		assert.True(t, removed)

		_, loaded, _ := ethCache.GetPendingTransaction(txHash)
		assert.False(t, loaded)
	})

	t.Run("ShouldCheckNow expiration logic", func(t *testing.T) {
		// override config for faster test
		cfg := newEthCacheConfig()
		cfg.PendingTxnCheckExemption = 1 * time.Second
		cfg.PendingTxnCheckInterval = 500 * time.Millisecond
		ethCache := newEthCache(cfg)

		txHash := common.HexToHash("0xddd")
		ethCache.AddPendingTransaction(txHash)

		time.Sleep(1100 * time.Millisecond)

		pending, loaded, expired := ethCache.GetPendingTransaction(txHash)
		assert.True(t, loaded)
		assert.True(t, expired, "should expire after exemption")

		pending.MarkChecked()
		_, _, expired = ethCache.GetPendingTransaction(txHash)
		assert.False(t, expired, "just checked, should not expire")

		time.Sleep(cfg.PendingTxnCheckInterval + 100*time.Millisecond)
		_, _, expired = ethCache.GetPendingTransaction(txHash)
		assert.True(t, expired, "should expire after interval")
	})

	t.Run("MarkChecked updates timestamp", func(t *testing.T) {
		ptx := &ethPendingTxn{
			createdAt: time.Now().Add(-5 * time.Minute),
		}

		exempt := 1 * time.Minute
		interval := 1 * time.Second

		assert.True(t, ptx.shouldCheckNow(exempt, interval), "should check when never checked")

		ptx.MarkChecked()
		assert.False(t, ptx.shouldCheckNow(exempt, interval), "just checked")
	})
}
