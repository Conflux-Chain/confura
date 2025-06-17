package cache

import (
	"math/big"
	"testing"

	cacheTypes "github.com/Conflux-Chain/confura-data-cache/types"
	"github.com/ethereum/go-ethereum/common"
	ethTypes "github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/openweb3/web3go/types"
	"github.com/stretchr/testify/assert"
)

var (
	nodeName = "testNode"

	chainId = big.NewInt(1030)
	toAddr  = common.HexToAddress("0xF0109fC8DF283027b6285cc889F5aA624EaC1F55")

	// Private key used to sign transactions for testing
	testPrivateKeyHex = "4f3edf983ac636a65a842ce7c78d9aa706d3b113b37c98b8dc6ef8c9c8c0d3c5"
)

func TestLazyRlpDecodedTxn(t *testing.T) {
	t.Parallel()

	t.Run("LoadEmptyData", func(t *testing.T) {
		lazy := newLazyRlpDecodedTxn([]byte{})
		txn, err := lazy.Load()
		assert.NoError(t, err)
		assert.Nil(t, txn)
	})

	t.Run("LoadLegacyTxnRLP", func(t *testing.T) {
		signedTx, encoded, err := buildSignedLegacyTx(
			0,
			big.NewInt(234567897654321),
			2000000,
			big.NewInt(1000000000),
		)
		assert.NoError(t, err)

		signer := ethTypes.LatestSignerForChainID(chainId)
		assert.NoError(t, err)

		sender, err := ethTypes.Sender(signer, signedTx)
		assert.NoError(t, err)

		// Construct a `lazyRlpDecodedTxn` and load it
		lazy := newLazyRlpDecodedTxn(encoded)

		loadedTx, err := lazy.Load()
		assert.NoError(t, err)

		// Verify the loaded transaction
		assert.Equal(t, chainId.Uint64(), loadedTx.ChainId().Uint64())
		assert.Equal(t, ethTypes.LegacyTxType, int(loadedTx.Type()))
		assert.Equal(t, toAddr.String(), loadedTx.To().String())
		assert.EqualValues(t, 0, loadedTx.Nonce())
		assert.EqualValues(t, 234567897654321, loadedTx.GasPrice().Uint64())
		assert.EqualValues(t, 2000000, loadedTx.Gas())
		assert.EqualValues(t, 1000000000, loadedTx.Value().Uint64())

		loadedTx2, err := lazy.Load()
		assert.NoError(t, err)
		assert.Same(t, loadedTx, loadedTx2, "should be cached and return the same pointer")

		// Build and check transaction detail
		txnDetail, err := buildTransactionDetail(loadedTx)
		assert.NoError(t, err)
		assert.NotNil(t, txnDetail)
		assert.Equal(t, ethTypes.LegacyTxType, int(*txnDetail.Type))
		assert.Equal(t, signedTx.Hash(), txnDetail.Hash)
		assert.Equal(t, sender, txnDetail.From)
		assert.Equal(t, toAddr.String(), txnDetail.To.String())
		assert.Equal(t, chainId.Uint64(), txnDetail.ChainID.Uint64())
		assert.EqualValues(t, 0, txnDetail.Nonce)
		assert.EqualValues(t, 1000000000, txnDetail.Value.Uint64())
		assert.EqualValues(t, 2000000, txnDetail.Gas)
		assert.EqualValues(t, 234567897654321, txnDetail.GasPrice.Uint64())
	})

	t.Run("LoadEIP1559Txn", func(t *testing.T) {
		signedTx, encoded, err := buildEIP1559Tx(
			0,
			big.NewInt(1000000000),
			big.NewInt(50000000000),
			21000,
			big.NewInt(1000000000),
		)
		assert.NoError(t, err)

		signer := ethTypes.LatestSignerForChainID(chainId)
		sender, err := ethTypes.Sender(signer, signedTx)
		assert.NoError(t, err)

		// Construct a `lazyRlpDecodedTxn` and load it
		lazy := newLazyRlpDecodedTxn(encoded)
		loadedTx, err := lazy.Load()
		assert.NoError(t, err)

		// Verify the loaded transaction
		assert.Equal(t, chainId.Uint64(), loadedTx.ChainId().Uint64())
		assert.Equal(t, ethTypes.DynamicFeeTxType, int(loadedTx.Type()))
		assert.Equal(t, toAddr.String(), loadedTx.To().String())
		assert.EqualValues(t, 0, loadedTx.Nonce())
		assert.EqualValues(t, 1000000000, loadedTx.GasTipCap().Uint64())
		assert.EqualValues(t, 50000000000, loadedTx.GasFeeCap().Uint64())
		assert.EqualValues(t, 21000, loadedTx.Gas())
		assert.EqualValues(t, 1000000000, loadedTx.Value().Uint64())

		loadedTx2, err := lazy.Load()
		assert.NoError(t, err)
		assert.Same(t, loadedTx, loadedTx2, "should be cached and return the same pointer")

		// Build and check transaction detail
		txnDetail, err := buildTransactionDetail(loadedTx)
		assert.NoError(t, err)
		assert.NotNil(t, txnDetail)
		assert.Equal(t, chainId.Uint64(), txnDetail.ChainID.Uint64())
		assert.Equal(t, ethTypes.DynamicFeeTxType, int(*txnDetail.Type))
		assert.Equal(t, signedTx.Hash(), txnDetail.Hash)
		assert.Equal(t, sender, txnDetail.From)
		assert.Equal(t, toAddr.String(), txnDetail.To.String())
		assert.Equal(t, chainId.Uint64(), txnDetail.ChainID.Uint64())
		assert.EqualValues(t, 0, txnDetail.Nonce)
		assert.EqualValues(t, 1000000000, txnDetail.Value.Uint64())
		assert.EqualValues(t, 1000000000, txnDetail.MaxPriorityFeePerGas.Uint64())
		assert.EqualValues(t, 50000000000, txnDetail.MaxFeePerGas.Uint64())
	})
}

func TestGetTransactionByHashWithFunc(t *testing.T) {
	t.Parallel()

	ethCache := newEthCache(newEthCacheConfig())

	t.Run("NoPendingTransactionCache", func(t *testing.T) {
		signedTx, _, err := buildEIP1559Tx(
			0,
			big.NewInt(1000000000),
			big.NewInt(50000000000),
			21000,
			big.NewInt(1000000000),
		)
		assert.NoError(t, err)

		expectedDetail, err := buildTransactionDetail(signedTx)
		assert.NoError(t, err)

		txHash := signedTx.Hash()

		rawGetterCalled := false
		rawGetter := func() (cacheTypes.Lazy[*types.TransactionDetail], error) {
			rawGetterCalled = true
			return cacheTypes.NewLazy(expectedDetail)
		}

		result, loaded, err := ethCache.GetTransactionByHashWithFunc(nodeName, txHash, rawGetter)
		assert.NoError(t, err)
		assert.False(t, loaded)
		assert.True(t, rawGetterCalled)

		txn, err := result.Load()
		assert.NoError(t, err)
		assert.Equal(t, txHash, txn.Hash)

		// Cache hit after first call
		result, loaded, err = ethCache.GetTransactionByHashWithFunc(nodeName, txHash,
			func() (cacheTypes.Lazy[*types.TransactionDetail], error) {
				t.Fatal("rawGetter should not be called on cache hit")
				return cacheTypes.Lazy[*types.TransactionDetail]{}, nil
			},
		)
		assert.NoError(t, err)
		assert.True(t, loaded)

		txn, err = result.Load()
		assert.NoError(t, err)
		assert.Equal(t, txHash, txn.Hash)
	})

	t.Run("PendingTransactionHit", func(t *testing.T) {
		signedTx, encoded, err := buildEIP1559Tx(
			1,
			big.NewInt(1000000000),
			big.NewInt(50000000000),
			21000,
			big.NewInt(1000000000),
		)
		assert.NoError(t, err)

		pendingHash := signedTx.Hash()
		ethCache.AddPendingTransaction(pendingHash, encoded)

		rawGetter := func() (cacheTypes.Lazy[*types.TransactionDetail], error) {
			t.Fatal("rawGetter should not be called for pending transaction")
			return cacheTypes.Lazy[*types.TransactionDetail]{}, nil
		}

		result, loaded, err := ethCache.GetTransactionByHashWithFunc(nodeName, pendingHash, rawGetter)
		assert.NoError(t, err)
		assert.True(t, loaded)

		txn, err := result.Load()
		assert.NoError(t, err)
		assert.Equal(t, pendingHash, txn.Hash)
	})
}

func TestGetTransactionReceiptWithFunc(t *testing.T) {
	ethCache := newEthCache(newEthCacheConfig())

	t.Run("NoPendingTransactionCache", func(t *testing.T) {
		txHash := common.HexToHash("0xabc123456")
		rawGetterCalled := false

		rawGetter := func() (cacheTypes.Lazy[*types.Receipt], error) {
			rawGetterCalled = true
			return cacheTypes.NewLazy(&types.Receipt{TransactionHash: txHash})
		}

		lazy, loaded, err := ethCache.GetTransactionReceiptWithFunc(nodeName, txHash, rawGetter)
		assert.NoError(t, err)
		assert.False(t, loaded)
		assert.True(t, rawGetterCalled)

		receipt, err := lazy.Load()
		assert.NoError(t, err)
		assert.Equal(t, txHash, receipt.TransactionHash)

		// Cache hit after first call
		result, loaded, err := ethCache.GetTransactionReceiptWithFunc(nodeName, txHash,
			func() (cacheTypes.Lazy[*types.Receipt], error) {
				t.Fatal("rawGetter should not be called on cache hit")
				return cacheTypes.Lazy[*types.Receipt]{}, nil
			},
		)
		assert.NoError(t, err)
		assert.True(t, loaded)

		receipt, err = result.Load()
		assert.NoError(t, err)
		assert.Equal(t, txHash, receipt.TransactionHash)
	})

	t.Run("PendingTransactionHit", func(t *testing.T) {
		signedTx, encoded, err := buildEIP1559Tx(
			0,
			big.NewInt(1000000000),
			big.NewInt(50000000000),
			21000,
			big.NewInt(1000000000),
		)
		assert.NoError(t, err)

		pendingHash := signedTx.Hash()
		ethCache.AddPendingTransaction(pendingHash, encoded)

		rawGetter := func() (cacheTypes.Lazy[*types.Receipt], error) {
			t.Fatal("rawGetter should not be called for pending transaction")
			return cacheTypes.Lazy[*types.Receipt]{}, nil
		}

		result, loaded, err := ethCache.GetTransactionReceiptWithFunc(nodeName, pendingHash, rawGetter)
		assert.NoError(t, err)
		assert.True(t, loaded)

		receipt, err := result.Load()
		assert.NoError(t, err)
		assert.Nil(t, receipt)
	})
}

// buildSignedLegacyTx returns a signed legacy transaction and its RLP-encoded bytes.
func buildSignedLegacyTx(nonce uint64, gasPrice *big.Int, gasLimit uint64, value *big.Int) (*types.Transaction, []byte, error) {
	privateKey, err := crypto.HexToECDSA(testPrivateKeyHex)
	if err != nil {
		return nil, nil, err
	}

	txData := ethTypes.LegacyTx{
		Nonce:    nonce,
		GasPrice: gasPrice,
		Gas:      gasLimit,
		To:       &toAddr,
		Value:    value,
	}
	tx := ethTypes.NewTx(&txData)

	signer := ethTypes.NewEIP155Signer(chainId)
	signedTx, err := ethTypes.SignTx(tx, signer, privateKey)
	if err != nil {
		return nil, nil, err
	}

	encoded, err := signedTx.MarshalBinary()
	if err != nil {
		return nil, nil, err
	}

	return signedTx, encoded, nil
}

// buildEIP1559Tx returns a signed EIP1559 transaction and its RLP-encoded bytes.
func buildEIP1559Tx(nonce uint64, gasTipCap, gasFeeCap *big.Int, gas uint64, value *big.Int) (*types.Transaction, []byte, error) {
	privateKey, err := crypto.HexToECDSA(testPrivateKeyHex)
	if err != nil {
		return nil, nil, err
	}

	// Construct a EIP1559 transaction
	txData := ethTypes.DynamicFeeTx{
		ChainID:   chainId,
		Nonce:     nonce,
		GasTipCap: gasTipCap,
		GasFeeCap: gasFeeCap,
		Gas:       gas,
		To:        &toAddr,
		Value:     value,
	}
	tx := ethTypes.NewTx(&txData)

	signer := ethTypes.LatestSignerForChainID(chainId)
	signedTx, err := ethTypes.SignTx(tx, signer, privateKey)
	if err != nil {
		return nil, nil, err
	}

	encoded, err := signedTx.MarshalBinary()
	if err != nil {
		return nil, nil, err
	}

	return signedTx, encoded, nil
}
