package cfxbridge

import (
	"math/big"

	"github.com/Conflux-Chain/go-conflux-sdk/types"
	"github.com/Conflux-Chain/go-conflux-sdk/types/cfxaddress"
	"github.com/Conflux-Chain/go-conflux-sdk/types/cmptutil"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	ethTypes "github.com/openweb3/web3go/types"
	"github.com/sirupsen/logrus"
)

var (
	txnStatusFailed  = hexutil.Uint64(1) // conflux tx status `failed`
	txnStatusSuccess = hexutil.Uint64(0) // conflux tx status `success`

	// conflux txn type
	txnTypeLegacy = hexutil.Uint64(types.TRANSACTION_TYPE_LEGACY)
	txnType1559   = hexutil.Uint64(types.TRANSACTION_TYPE_1559)
	txnType2930   = hexutil.Uint64(types.TRANSACTION_TYPE_2930)

	// conflux space
	spaceEVM = types.SPACE_EVM
)

func NormalizeBig(value *big.Int, err error) (*hexutil.Big, error) {
	if err != nil {
		return nil, err
	}

	return types.NewBigIntByRaw(value), nil
}

func ConvertHashNullable(value *common.Hash) *types.Hash {
	if value == nil {
		return nil
	}

	hash := types.Hash(value.Hex())
	return &hash
}

func ConvertHash(value common.Hash) types.Hash {
	return types.Hash(value.Hex())
}

func ConvertAddressNullable(value *common.Address, ethNetworkId uint32) *types.Address {
	if value == nil {
		return nil
	}

	address, _ := cfxaddress.NewFromCommon(*value, ethNetworkId)
	return &address
}

func ConvertAddress(value common.Address, ethNetworkId uint32) types.Address {
	address, _ := cfxaddress.NewFromCommon(value, ethNetworkId)
	return address
}

func ConvertAddresses(addresses []common.Address, ethNetworkId uint32) []types.Address {
	cAddrs := []types.Address{}
	for _, addr := range addresses {
		cAddrs = append(cAddrs, ConvertAddress(addr, ethNetworkId))
	}

	return cAddrs
}

func ConvertTxStatus(value *uint64) hexutil.Uint64 {
	v := ConvertTxStatusNullable(value)
	if v == nil { // unkown status? regarded as `failed` anyway
		return txnStatusFailed
	}

	return *v
}

func ConvertTxStatusNullable(value *uint64) (status *hexutil.Uint64) {
	if value == nil {
		return nil
	}

	switch {
	case *value == 0: // eth tx status `failed`
		status = &txnStatusFailed
	case *value == 1: // eth tx status `success`
		status = &txnStatusSuccess
	default:
		logrus.WithField("ethTxStatus", *value).Error("Unexpected tx status from eth space")
	}

	return
}

// Deduce transaction type from other transaction fields but without furthur validation.
func DeduceTxnType(txn *ethTypes.TransactionDetail) *hexutil.Uint64 {
	switch {
	case txn.MaxFeePerGas != nil || txn.MaxPriorityFeePerGas != nil:
		return &txnType1559
	case txn.Accesses != nil:
		return &txnType2930
	default:
		return &txnTypeLegacy
	}
}

func ConvertTx(tx *ethTypes.TransactionDetail, ethNetworkId uint32) *types.Transaction {
	if tx == nil {
		return nil
	}

	chainId := HexBig0
	if tx.ChainID != nil {
		chainId = types.NewBigIntByRaw(tx.ChainID)
	}

	return &types.Transaction{
		TransactionType:      DeduceTxnType(tx),
		Hash:                 types.Hash(tx.Hash.Hex()),
		Nonce:                types.NewBigInt(tx.Nonce),
		BlockHash:            ConvertHashNullable(tx.BlockHash),
		TransactionIndex:     (*hexutil.Uint64)(tx.TransactionIndex),
		From:                 ConvertAddress(tx.From, ethNetworkId),
		To:                   ConvertAddressNullable(tx.To, ethNetworkId),
		Value:                types.NewBigIntByRaw(tx.Value),
		GasPrice:             types.NewBigIntByRaw(tx.GasPrice),
		Gas:                  types.NewBigInt(tx.Gas),
		ContractCreated:      ConvertAddressNullable(tx.Creates, ethNetworkId),
		Data:                 hexutil.Encode(tx.Input),
		StorageLimit:         HexBig0,
		EpochHeight:          HexBig0,
		ChainID:              chainId,
		Status:               ConvertTxStatusNullable(tx.Status),
		AccessList:           types.ConvertEthAccessListToCfx(tx.Accesses, ethNetworkId),
		MaxPriorityFeePerGas: types.NewBigIntByRaw(tx.MaxPriorityFeePerGas),
		MaxFeePerGas:         types.NewBigIntByRaw(tx.MaxFeePerGas),
		V:                    types.NewBigIntByRaw(tx.V),
		R:                    types.NewBigIntByRaw(tx.R),
		S:                    types.NewBigIntByRaw(tx.S),
		YParity:              (*hexutil.Uint64)(tx.YParity),
	}
}

func ConvertBlockHeader(block *ethTypes.Block, ethNetworkId uint32) *types.BlockHeader {
	if block == nil {
		return nil
	}

	referees := make([]types.Hash, len(block.Uncles))
	for i := range block.Uncles {
		referees[i] = types.Hash(block.Uncles[i].Hex())
	}

	extraData := block.ExtraData
	if extraData == nil {
		extraData = []byte{}
	}

	return &types.BlockHeader{
		Hash:                  types.Hash(block.Hash.Hex()),
		ParentHash:            types.Hash(block.ParentHash.Hex()),
		Height:                types.NewBigIntByRaw(block.Number),
		Miner:                 ConvertAddress(block.Miner, ethNetworkId),
		DeferredStateRoot:     types.Hash(block.StateRoot.Hex()),
		DeferredReceiptsRoot:  types.Hash(block.ReceiptsRoot.Hex()),
		DeferredLogsBloomHash: types.Hash(hexutil.Encode(block.LogsBloom.Bytes())),
		Blame:                 0,
		TransactionsRoot:      types.Hash(block.TransactionsRoot.Hex()),
		EpochNumber:           types.NewBigIntByRaw(block.Number),
		BlockNumber:           types.NewBigIntByRaw(block.Number),
		GasLimit:              types.NewBigInt(block.GasLimit),
		GasUsed:               types.NewBigInt(block.GasUsed),
		BaseFeePerGas:         types.NewBigIntByRaw(block.BaseFeePerGas),
		Timestamp:             types.NewBigInt(block.Timestamp),
		Difficulty:            types.NewBigIntByRaw(block.Difficulty),
		PowQuality:            HexBig0,
		RefereeHashes:         referees,
		Adaptive:              false,
		Nonce:                 types.NewBigInt(block.Nonce.Uint64()),
		Size:                  types.NewBigInt(block.Size),
		Custom:                []cmptutil.Bytes{extraData},
	}
}

func ConvertBlock(block *ethTypes.Block, ethNetworkId uint32) *types.Block {
	if block == nil {
		return nil
	}

	blockTxs := block.Transactions.Transactions()

	txs := make([]types.Transaction, len(blockTxs))
	for i := range blockTxs {
		txs[i] = *ConvertTx(&blockTxs[i], ethNetworkId)
	}

	return &types.Block{
		BlockHeader:  *ConvertBlockHeader(block, ethNetworkId),
		Transactions: txs,
	}
}

func ConvertBlockSummary(block *ethTypes.Block, ethNetworkId uint32) *types.BlockSummary {
	if block == nil {
		return nil
	}

	blockTxs := block.Transactions.Hashes()

	txs := make([]types.Hash, len(blockTxs))
	for i := range blockTxs {
		txs[i] = types.Hash(blockTxs[i].Hex())
	}

	return &types.BlockSummary{
		BlockHeader:  *ConvertBlockHeader(block, ethNetworkId),
		Transactions: txs,
	}
}

func ConvertLog(log *ethTypes.Log, ethNetworkId uint32) *types.Log {
	if log == nil {
		return nil
	}

	topics := make([]types.Hash, len(log.Topics))
	for i := range log.Topics {
		topics[i] = types.Hash(log.Topics[i].Hex())
	}

	return &types.Log{
		Address:             ConvertAddress(log.Address, ethNetworkId),
		Topics:              topics,
		Data:                log.Data,
		BlockHash:           ConvertHashNullable(&log.BlockHash),
		EpochNumber:         types.NewBigInt(log.BlockNumber),
		TransactionHash:     ConvertHashNullable(&log.TxHash),
		TransactionIndex:    types.NewBigInt(uint64(log.TxIndex)),              // tx index in block
		LogIndex:            types.NewBigInt(uint64(log.Index)),                // log index in block
		TransactionLogIndex: types.NewBigInt(uint64(*log.TransactionLogIndex)), // log index in tx
		Space:               &spaceEVM,
	}
}

func ConvertReceipt(receipt *ethTypes.Receipt, ethNetworkId uint32) *types.TransactionReceipt {
	if receipt == nil {
		return nil
	}

	logs := make([]types.Log, len(receipt.Logs))
	for i := range receipt.Logs {
		logs[i] = *ConvertLog(receipt.Logs[i], ethNetworkId)
	}

	var stateRoot types.Hash
	if len(receipt.Root) == 0 {
		stateRoot = types.Hash(common.Hash{}.Hex())
	} else {
		stateRoot = types.Hash(hexutil.Encode(receipt.Root))
	}

	gasFee := new(big.Int).Mul(
		new(big.Int).SetUint64(receipt.EffectiveGasPrice),
		new(big.Int).SetUint64(receipt.GasUsed),
	)

	return &types.TransactionReceipt{
		Type:                    (*hexutil.Uint64)(receipt.Type),
		TransactionHash:         types.Hash(receipt.TransactionHash.Hex()),
		Index:                   hexutil.Uint64(receipt.TransactionIndex),
		BlockHash:               types.Hash(receipt.BlockHash.Hex()),
		EpochNumber:             types.NewUint64(receipt.BlockNumber),
		From:                    ConvertAddress(receipt.From, ethNetworkId),
		To:                      ConvertAddressNullable(receipt.To, ethNetworkId),
		GasUsed:                 types.NewBigInt(receipt.GasUsed),
		AccumulatedGasUsed:      types.NewBigInt(receipt.CumulativeGasUsed),
		GasFee:                  types.NewBigIntByRaw(gasFee),
		EffectiveGasPrice:       types.NewBigInt(receipt.EffectiveGasPrice),
		ContractCreated:         ConvertAddressNullable(receipt.ContractAddress, ethNetworkId),
		Logs:                    logs,
		LogsBloom:               types.Bloom(hexutil.Encode(receipt.LogsBloom.Bytes())),
		StateRoot:               stateRoot,
		OutcomeStatus:           ConvertTxStatus(receipt.Status),
		TxExecErrorMsg:          receipt.TxExecErrorMsg,
		GasCoveredBySponsor:     false,
		StorageCoveredBySponsor: false,
		StorageCollateralized:   0,
		StorageReleased:         emptyStorageChangeList,
		BurntGasFee:             types.NewBigIntByRaw(receipt.BurntGasFee),
	}
}

func ConvertLogFilter(fq *ethTypes.FilterQuery, ethNetworkId uint32) *types.LogFilter {
	lf := &types.LogFilter{}

	// convert log filter addresses
	for _, fqa := range fq.Addresses {
		lfa := ConvertAddress(fqa, ethNetworkId)
		lf.Address = append(lf.Address, lfa)
	}

	// convert log filter topics
	for _, fqts := range fq.Topics {
		lfts := []types.Hash{}
		for _, fqth := range fqts {
			lfts = append(lfts, ConvertHash(fqth))
		}
		lf.Topics = append(lf.Topics, lfts)
	}

	// convert log filter block hashes
	lfBlockHash := ConvertHashNullable(fq.BlockHash)
	if lfBlockHash != nil {
		lf.BlockHashes = []types.Hash{*lfBlockHash}
	}

	// convert log filter block number
	if fq.FromBlock != nil {
		lf.FromBlock = (*hexutil.Big)(big.NewInt(fq.FromBlock.Int64()))
	}

	if fq.ToBlock != nil {
		lf.ToBlock = (*hexutil.Big)(big.NewInt(fq.ToBlock.Int64()))
	}

	return lf
}
