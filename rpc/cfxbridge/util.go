package cfxbridge

import (
	"math/big"

	"github.com/Conflux-Chain/go-conflux-sdk/types"
	"github.com/Conflux-Chain/go-conflux-sdk/types/cfxaddress"
	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	ethTypes "github.com/ethereum/go-ethereum/core/types"
)

var (
	defaultReceiptErrMsg = "transaction reverted"
)

func (api *CfxAPI) normalizeBig(val *big.Int, err error) (*hexutil.Big, error) {
	if err != nil {
		return nil, err
	}

	return types.NewBigIntByRaw(val), nil
}

func (api *CfxAPI) normalizeUint64(val uint64, err error) (*hexutil.Big, error) {
	if err != nil {
		return nil, err
	}

	return types.NewBigInt(val), nil
}

func (api *CfxAPI) convertTx(tx *ethTypes.Transaction) *types.Transaction {
	if tx == nil {
		return nil
	}

	var to *cfxaddress.Address
	if ethAddr := tx.To(); ethAddr != nil {
		cfxAddr := cfxaddress.MustNewFromCommon(*ethAddr, api.networkId)
		to = &cfxAddr
	}

	v, r, s := tx.RawSignatureValues()

	// TODO ethclient missed some fields, wait for web3go from SDK team.
	// Including: BlockHash, TransactionIndex, From, ContractCreated, Status
	return &types.Transaction{
		Hash:             types.Hash(tx.Hash().Hex()),
		Nonce:            types.NewBigInt(tx.Nonce()),
		BlockHash:        nil,
		TransactionIndex: nil,
		From:             cfxaddress.MustNewFromCommon(common.Address{}, api.networkId),
		To:               to,
		Value:            types.NewBigIntByRaw(tx.Value()),
		GasPrice:         types.NewBigIntByRaw(tx.GasPrice()),
		Gas:              types.NewBigInt(tx.Gas()),
		ContractCreated:  nil,
		Data:             hexutil.Encode(tx.Data()),
		StorageLimit:     HexBig0,
		EpochHeight:      HexBig0,
		ChainID:          types.NewBigIntByRaw(tx.ChainId()),
		Status:           nil,
		V:                types.NewBigIntByRaw(v),
		R:                types.NewBigIntByRaw(r),
		S:                types.NewBigIntByRaw(s),
	}
}

func (api *CfxAPI) convertBlockHeader(block *ethTypes.Block) *types.BlockHeader {
	if block == nil {
		return nil
	}

	var uncles []types.Hash
	for _, v := range block.Uncles() {
		uncles = append(uncles, types.Hash(v.Hash().Hex()))
	}

	var custom [][]byte
	if extra := block.Extra(); len(extra) > 0 {
		custom = append(custom, extra)
	}

	return &types.BlockHeader{
		Hash:                  types.Hash(block.Hash().Hex()),
		ParentHash:            types.Hash(block.ParentHash().Hex()),
		Height:                types.NewBigIntByRaw(block.Number()),
		Miner:                 cfxaddress.MustNewFromCommon(block.Coinbase(), api.networkId),
		DeferredStateRoot:     types.Hash(block.Root().Hex()),
		DeferredReceiptsRoot:  types.Hash(block.ReceiptHash().Hex()),
		DeferredLogsBloomHash: types.Hash(hexutil.Encode(block.Bloom().Bytes())),
		Blame:                 0,
		TransactionsRoot:      types.Hash(block.TxHash().Hex()),
		EpochNumber:           types.NewBigIntByRaw(block.Number()),
		BlockNumber:           types.NewBigIntByRaw(block.Number()),
		GasLimit:              types.NewBigInt(block.GasLimit()),
		GasUsed:               types.NewBigInt(block.GasUsed()),
		Timestamp:             types.NewBigInt(block.Time()),
		Difficulty:            types.NewBigIntByRaw(block.Difficulty()),
		PowQuality:            HexBig0,
		RefereeHashes:         uncles,
		Adaptive:              false,
		Nonce:                 types.NewBigInt(block.Nonce()),
		Size:                  types.NewBigInt(uint64(block.Size())),
		Custom:                custom,
	}
}

func (api *CfxAPI) normalizeBlock(block *ethTypes.Block, err error) (*types.Block, error) {
	if err == ethereum.NotFound {
		return nil, nil
	}

	if err != nil {
		return nil, err
	}

	if block == nil {
		return nil, nil
	}

	var txs []types.Transaction
	for _, v := range block.Transactions() {
		txs = append(txs, *api.convertTx(v))
	}

	return &types.Block{
		BlockHeader:  *api.convertBlockHeader(block),
		Transactions: txs,
	}, nil
}

func (api *CfxAPI) normalizeBlockSummary(block *ethTypes.Block, err error) (*types.BlockSummary, error) {
	if err == ethereum.NotFound {
		return nil, nil
	}

	if err != nil {
		return nil, err
	}

	if block == nil {
		return nil, nil
	}

	var txs []types.Hash
	for _, v := range block.Transactions() {
		txs = append(txs, api.convertTx(v).Hash)
	}

	return &types.BlockSummary{
		BlockHeader:  *api.convertBlockHeader(block),
		Transactions: txs,
	}, nil
}

func (api *CfxAPI) convertLog(log *ethTypes.Log) *types.Log {
	if log == nil {
		return nil
	}

	var topics []types.Hash
	for i := range log.Topics {
		topics = append(topics, types.Hash(log.Topics[i].Hex()))
	}

	blockHash := types.Hash(log.BlockHash.Hex())
	txHash := types.Hash(log.TxHash.Hex())

	return &types.Log{
		Address:             cfxaddress.MustNewFromCommon(log.Address, api.networkId),
		Topics:              topics,
		Data:                log.Data,
		BlockHash:           &blockHash,
		EpochNumber:         types.NewBigInt(log.BlockNumber),
		TransactionHash:     &txHash,
		TransactionIndex:    types.NewBigInt(uint64(log.TxIndex)), // tx index in block
		LogIndex:            types.NewBigInt(uint64(log.Index)),   // log index in block
		TransactionLogIndex: HexBig0,                              // log index in tx, not supported in eth space
	}
}

func (api *CfxAPI) convertReceipt(receipt *ethTypes.Receipt) *types.TransactionReceipt {
	if receipt == nil {
		return nil
	}

	var contractCreated *cfxaddress.Address
	if receipt.ContractAddress != (common.Address{}) {
		cfxAddr := cfxaddress.MustNewFromCommon(receipt.ContractAddress, api.networkId)
		contractCreated = &cfxAddr
	}

	var logs []types.Log
	for _, v := range receipt.Logs {
		logs = append(logs, *api.convertLog(v))
	}

	var outcomeStatus hexutil.Uint64
	var errMsg *string
	if receipt.Status == ethTypes.ReceiptStatusFailed {
		outcomeStatus = 1
		errMsg = &defaultReceiptErrMsg
	}

	// TODO ethclient missed some fields, wait for web3go from SDK team.
	// Including: From, To, GasFee, TxExecErrorMsg
	return &types.TransactionReceipt{
		TransactionHash:         types.Hash(receipt.TxHash.Hex()),
		Index:                   hexutil.Uint64(receipt.TransactionIndex),
		BlockHash:               types.Hash(receipt.BlockHash.Hex()),
		EpochNumber:             types.NewUint64(receipt.BlockNumber.Uint64()),
		From:                    cfxaddress.MustNewFromCommon(common.Address{}, api.networkId),
		To:                      nil,
		GasUsed:                 types.NewBigInt(receipt.GasUsed),
		GasFee:                  HexBig0,
		ContractCreated:         contractCreated,
		Logs:                    logs,
		LogsBloom:               types.Bloom(hexutil.Encode(receipt.Bloom.Bytes())),
		StateRoot:               types.Hash(hexutil.Encode(receipt.PostState)),
		OutcomeStatus:           outcomeStatus,
		TxExecErrorMsg:          errMsg,
		GasCoveredBySponsor:     false,
		StorageCoveredBySponsor: false,
		StorageCollateralized:   0,
		StorageReleased:         nil,
	}
}
