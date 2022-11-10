package util

import (
	"reflect"
	"regexp"
	"strconv"

	sdk "github.com/Conflux-Chain/go-conflux-sdk"
	"github.com/Conflux-Chain/go-conflux-sdk/types"
	"github.com/ethereum/go-ethereum/common"
	ethtypes "github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/rpc"
	"github.com/openweb3/web3go"
	web3goTypes "github.com/openweb3/web3go/types"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

var (
	hashRegexp *regexp.Regexp = regexp.MustCompile("^0x([A-Fa-f0-9]{64})$")

	ethHardforkBlockNumberDevnet    = rpc.BlockNumber(1)
	ethChainId2HardforkBlockNumbers = map[uint64]rpc.BlockNumber{
		1030: 36935000, //mainnet
		71:   61465000, // testnet
	}
)

// IsValidHashStr validates if the search (block/tx) hash contains the right characters
// and in the right length.
func IsValidHashStr(hashStr string) bool {
	return hashRegexp.MatchString(hashStr)
}

func GetShortIdOfHash(hash string) uint64 {
	if !IsValidHashStr(hash) {
		logrus.WithField("hash", hash).Error("Failed to get short id of an invalid hash")
		return 0
	}

	// first 8 bytes of hex string with 0x prefixed
	id, err := strconv.ParseUint(hash[2:18], 16, 64)
	if err != nil {
		logrus.WithError(err).WithField("hash", hash).Fatalf("Failed convert hash to short id")
	}

	return id
}

func GetSummaryOfBlock(block *types.Block) *types.BlockSummary {
	summary := types.BlockSummary{
		BlockHeader:  block.BlockHeader,
		Transactions: make([]types.Hash, 0, len(block.Transactions)),
	}

	for _, tx := range block.Transactions {
		summary.Transactions = append(summary.Transactions, tx.Hash)
	}

	return &summary
}

// StripLogExtraFields strips extra unnecessary fields from logs to comply with fullnode rpc
func StripLogExtraFieldsForRPC(logs []types.Log) {
	for i := 0; i < len(logs); i++ {
		log := &logs[i]

		log.BlockHash, log.EpochNumber = nil, nil
		log.TransactionHash, log.TransactionIndex = nil, nil
		log.LogIndex, log.TransactionLogIndex = nil, nil
	}
}

// ConvertToNumberedEpoch converts named epoch to numbered epoch if necessary
func ConvertToNumberedEpoch(cfx sdk.ClientOperator, epoch *types.Epoch) (*types.Epoch, error) {
	if epoch == nil {
		return nil, errors.New("named epoch must be provided")
	}

	if _, ok := epoch.ToInt(); ok { // already a numbered epoch
		return epoch, nil
	}

	epochNum, err := cfx.GetEpochNumber(epoch)
	if err != nil {
		return nil, errors.WithMessagef(
			err, "failed to get epoch number for named epoch %v", epoch,
		)
	}

	return types.NewEpochNumber(epochNum), nil
}

// ConvertToHashSlice convert string slice to hash slice
func ConvertToHashSlice(ss []string) []types.Hash {
	res := make([]types.Hash, 0, len(ss))

	for i := 0; i < len(ss); i++ {
		res = append(res, types.Hash(ss[i]))
	}

	return res
}

// NormalizeEthBlockNumber normalizes ETH block number to be positive if necessary
func NormalizeEthBlockNumber(
	w3c *web3go.Client, blockNum *rpc.BlockNumber, hardforkBlockNumber rpc.BlockNumber,
) (*rpc.BlockNumber, error) {
	if blockNum == nil {
		return nil, errors.New("block number must be provided")
	}

	// after eSpace hardfork
	if *blockNum > hardforkBlockNumber {
		return blockNum, nil
	}

	// before eSpace hardfork => hardfork
	if *blockNum > 0 {
		return &hardforkBlockNumber, nil
	}

	// earliest => hardfork
	if *blockNum == rpc.EarliestBlockNumber {
		return &hardforkBlockNumber, nil
	}

	// pending or latest => latest_mined or latest_state
	block, err := w3c.Eth.BlockByNumber(*blockNum, false)
	if err != nil {
		return nil, errors.WithMessage(err, "failed to normalize block number")
	}

	// !!! eth rpc may return nil for `pending` and `earlist` block number
	if block == nil {
		blockText, _ := blockNum.MarshalText()
		return nil, errors.Errorf("unknown block number (%v)", string(blockText))
	}

	blockNo := rpc.BlockNumber(block.Number.Int64())
	return &blockNo, nil
}

func IsZeroHash(hash *common.Hash) bool {
	return hash == nil || reflect.DeepEqual(hash, &common.Hash{})
}

func IsTxExecutedInBlock(tx *types.Transaction) bool {
	return tx != nil && tx.BlockHash != nil && tx.Status != nil && *tx.Status < 2
}

// IsEmptyBlock checks if block contains any executed transaction(s)
func IsEmptyBlock(block *types.Block) bool {
	for _, tx := range block.Transactions {
		if IsTxExecutedInBlock(&tx) {
			return false
		}
	}

	return true
}

// IsEip155Tx check if the EVM transaction is compliant to EIP155
func IsEip155Tx(tx *web3goTypes.TransactionDetail) bool {
	if tx.V != nil && tx.V.Uint64() >= 35 {
		return true
	}

	return false
}

// IsLegacyEthTx check if the EVM transaction is legacy (pre EIP155)
func IsLegacyEthTx(tx *web3goTypes.TransactionDetail) bool {
	if tx.V != nil && tx.V.Uint64() == 27 || tx.V.Uint64() == 28 {
		return true
	}

	return false
}

// IsSuccessEthTx check if the EVM transaction is success
func IsSuccessEthTx(tx *web3goTypes.TransactionDetail) bool {
	return tx.Status != nil && *tx.Status == ethtypes.ReceiptStatusSuccessful
}

// GetEthHardforkBlockNumber gets eSpace hardfork block number
func GetEthHardforkBlockNumber(chainId uint64) rpc.BlockNumber {
	if v, ok := ethChainId2HardforkBlockNumbers[chainId]; ok {
		return v
	}

	return ethHardforkBlockNumberDevnet
}
