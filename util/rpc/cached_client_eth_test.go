package rpc

import (
	"math/big"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	ethTypes "github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/openweb3/web3go/types"
	"github.com/stretchr/testify/assert"
)

var (
	// Test transaction parameters
	chainId   = big.NewInt(1030)
	toAddr    = common.HexToAddress("0xF0109fC8DF283027b6285cc889F5aA624EaC1F55")
	gasPrice  = big.NewInt(234567897654321)
	gasLimit  = uint64(2000000)
	value     = big.NewInt(1000000000)
	gasTipCap = big.NewInt(1000000000)
	gasFeeCap = big.NewInt(50000000000)
	data      = []byte{0x1, 0x2}

	// Private key used to sign transactions for testing
	testPrivateKeyHex = "4f3edf983ac636a65a842ce7c78d9aa706d3b113b37c98b8dc6ef8c9c8c0d3c5"
)

func TestBuildSignedTransactionDetail(t *testing.T) {
	t.Run("LegacyTxn", func(t *testing.T) {
		signedTx, _, err := buildSignedLegacyTx(0)
		assert.NoError(t, err)

		signer := ethTypes.LatestSignerForChainID(chainId)
		assert.NoError(t, err)

		sender, err := ethTypes.Sender(signer, signedTx)
		assert.NoError(t, err)

		txn, err := buildSignedTransactionDetail(signedTx)
		assert.NoError(t, err)
		assert.NotNil(t, txn)
		assert.Equal(t, ethTypes.LegacyTxType, int(*txn.Type))
		assert.Equal(t, signedTx.Hash(), txn.Hash)
		assert.Equal(t, sender, txn.From)
		assert.Equal(t, toAddr.String(), txn.To.String())
		assert.Equal(t, chainId.Uint64(), txn.ChainID.Uint64())
		assert.EqualValues(t, 0, txn.Nonce)
		assert.EqualValues(t, value.Uint64(), txn.Value.Uint64())
		assert.EqualValues(t, gasLimit, txn.Gas)
		assert.EqualValues(t, gasPrice.Uint64(), txn.GasPrice.Uint64())
		assert.EqualValues(t, signedTx.Data(), txn.Input)

		v, r, s := signedTx.RawSignatureValues()
		assert.Equal(t, r.String(), txn.R.String())
		assert.Equal(t, s.String(), txn.S.String())
		assert.Equal(t, v.String(), txn.V.String())
	})

	t.Run("EIP1559Txn", func(t *testing.T) {
		signedTx, _, err := buildEIP1559Tx(0)
		assert.NoError(t, err)

		signer := ethTypes.LatestSignerForChainID(chainId)
		sender, err := ethTypes.Sender(signer, signedTx)
		assert.NoError(t, err)

		txn, err := buildSignedTransactionDetail(signedTx)
		assert.NoError(t, err)
		assert.NotNil(t, txn)
		assert.Equal(t, chainId.Uint64(), txn.ChainID.Uint64())
		assert.Equal(t, ethTypes.DynamicFeeTxType, int(*txn.Type))
		assert.Equal(t, signedTx.Hash(), txn.Hash)
		assert.Equal(t, sender, txn.From)
		assert.Equal(t, toAddr.String(), txn.To.String())
		assert.Equal(t, chainId.Uint64(), txn.ChainID.Uint64())
		assert.EqualValues(t, 0, txn.Nonce)
		assert.EqualValues(t, value.Uint64(), txn.Value.Uint64())
		assert.EqualValues(t, gasTipCap.Uint64(), txn.MaxPriorityFeePerGas.Uint64())
		assert.EqualValues(t, gasFeeCap.Uint64(), txn.MaxFeePerGas.Uint64())
		assert.EqualValues(t, signedTx.Data(), txn.Input)

		v, r, s := signedTx.RawSignatureValues()
		assert.Equal(t, r.String(), txn.R.String())
		assert.Equal(t, s.String(), txn.S.String())
		assert.Equal(t, v.String(), txn.V.String())
	})

	t.Run("InvalidSender", func(t *testing.T) {
		unsignedTxn := ethTypes.NewTransaction(1, toAddr, value, gasLimit, gasPrice, data)
		// Unsigned transaction will fail sender recovery
		txn, err := buildSignedTransactionDetail(unsignedTxn)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to recover transaction sender")
		assert.Nil(t, txn)
	})
}

// buildSignedLegacyTx returns a signed legacy transaction and its RLP-encoded bytes.
func buildSignedLegacyTx(nonce uint64) (*types.Transaction, []byte, error) {
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
		Data:     data,
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
func buildEIP1559Tx(nonce uint64) (*types.Transaction, []byte, error) {
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
		Gas:       gasLimit,
		To:        &toAddr,
		Value:     value,
		Data:      data,
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
