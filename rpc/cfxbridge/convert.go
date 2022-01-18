package cfxbridge

import (
	"github.com/Conflux-Chain/go-conflux-sdk/types"
	"github.com/Conflux-Chain/go-conflux-sdk/types/cfxaddress"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	gethTypes "github.com/ethereum/go-ethereum/core/types"
	ethTypes "github.com/openweb3/web3go/types"
)

func (api *CfxAPI) convertHashNullable(value *common.Hash) *types.Hash {
	if value == nil {
		return nil
	}

	hash := types.Hash(value.Hex())
	return &hash
}

func (api *CfxAPI) convertAddressNullable(value *common.Address) *types.Address {
	if value == nil {
		return nil
	}

	address, _ := cfxaddress.NewFromCommon(*value, api.networkId)
	return &address
}

func (api *CfxAPI) convertAddress(value common.Address) types.Address {
	address, _ := cfxaddress.NewFromCommon(value, api.networkId)
	return address
}

func (api *CfxAPI) convertTx(tx *ethTypes.Transaction) *types.Transaction {
	if tx == nil {
		return nil
	}

	// TODO ethclient missed some fields, wait for web3go from SDK team.
	// Including: ContractCreated, Status
	return &types.Transaction{
		Hash:             types.Hash(tx.Hash.Hex()),
		Nonce:            types.NewBigInt(uint64(tx.Nonce)),
		BlockHash:        api.convertHashNullable(tx.BlockHash),
		TransactionIndex: tx.TransactionIndex,
		From:             api.convertAddress(tx.From),
		To:               api.convertAddressNullable(tx.To),
		Value:            tx.Value,
		GasPrice:         tx.GasPrice,
		Gas:              types.NewBigInt(uint64(tx.Gas)),
		ContractCreated:  nil,
		Data:             hexutil.Encode(tx.Input),
		StorageLimit:     HexBig0,
		EpochHeight:      HexBig0,
		ChainID:          tx.ChainID,
		Status:           nil,
		V:                tx.V,
		R:                tx.R,
		S:                tx.S,
	}
}

func (api *CfxAPI) convertBlockHeader(block *ethTypes.Block) *types.BlockHeader {
	if block == nil {
		return nil
	}

	referees := make([]types.Hash, len(block.Uncles))
	for i := range block.Uncles {
		referees[i] = types.Hash(block.Uncles[i].Hex())
	}

	custom := make([][]byte, 1)
	if len(block.ExtraData) > 0 {
		custom[0] = block.ExtraData
	}

	if block.LogsBloom == nil {
		block.LogsBloom = &emptyLogsBloom
	}

	// TODO eth space missed some fields, including: Nonce
	return &types.BlockHeader{
		Hash:                  types.Hash(block.Hash.Hex()),
		ParentHash:            types.Hash(block.ParentHash.Hex()),
		Height:                block.Number,
		Miner:                 api.convertAddress(block.Author),
		DeferredStateRoot:     types.Hash(block.StateRoot.Hex()),
		DeferredReceiptsRoot:  types.Hash(block.ReceiptsRoot.Hex()),
		DeferredLogsBloomHash: types.Hash(hexutil.Encode(block.LogsBloom.Bytes())),
		Blame:                 0,
		TransactionsRoot:      types.Hash(block.TransactionsRoot.Hex()),
		EpochNumber:           block.Number,
		BlockNumber:           block.Number,
		GasLimit:              block.GasLimit,
		GasUsed:               block.GasUsed,
		Timestamp:             block.Timestamp,
		Difficulty:            block.Difficulty,
		PowQuality:            HexBig0,
		RefereeHashes:         referees,
		Adaptive:              false,
		Nonce:                 HexBig0,
		Size:                  block.Size,
		Custom:                custom,
	}
}

func (api *CfxAPI) convertBlock(block *ethTypes.Block) *types.Block {
	if block == nil {
		return nil
	}

	txs := make([]types.Transaction, len(block.Transactions))
	for i := range block.Transactions {
		txs[i] = *api.convertTx(&block.Transactions[i])
	}

	return &types.Block{
		BlockHeader:  *api.convertBlockHeader(block),
		Transactions: txs,
	}
}

func (api *CfxAPI) convertBlockSummary(block *ethTypes.Block) *types.BlockSummary {
	if block == nil {
		return nil
	}

	txs := make([]types.Hash, len(block.Transactions))
	for i := range block.Transactions {
		txs[i] = types.Hash(block.Transactions[i].Hash.Hex())
	}

	return &types.BlockSummary{
		BlockHeader:  *api.convertBlockHeader(block),
		Transactions: txs,
	}
}

func (api *CfxAPI) convertLog(log *ethTypes.Log) *types.Log {
	if log == nil {
		return nil
	}

	topics := make([]types.Hash, len(log.Topics))
	for i := range log.Topics {
		topics[i] = types.Hash(log.Topics[i].Hex())
	}

	return &types.Log{
		Address:             api.convertAddress(log.Address),
		Topics:              topics,
		Data:                log.Data,
		BlockHash:           api.convertHashNullable(&log.BlockHash),
		EpochNumber:         types.NewBigInt(log.BlockNumber),
		TransactionHash:     api.convertHashNullable(&log.TxHash),
		TransactionIndex:    types.NewBigInt(uint64(log.TxIndex)), // tx index in block
		LogIndex:            types.NewBigInt(uint64(log.Index)),   // log index in block
		TransactionLogIndex: HexBig0,                              // log index in tx, not supported in eth space
	}
}

func (api *CfxAPI) convertReceipt(receipt *ethTypes.Receipt) *types.TransactionReceipt {
	if receipt == nil {
		return nil
	}

	logs := make([]types.Log, len(receipt.Logs))
	for i := range receipt.Logs {
		logs[i] = *api.convertLog(&receipt.Logs[i])
	}

	// StatusCode should always be available
	var outcomeStatus hexutil.Uint64
	var errMsg *string
	if *receipt.StatusCode == hexutil.Uint64(gethTypes.ReceiptStatusFailed) {
		outcomeStatus = 1
		errMsg = &defaultReceiptErrMsg
	}

	// TODO ethclient missed some fields, wait for web3go from SDK team.
	// Including: GasFee (price * gasCharged?), TxExecErrorMsg
	return &types.TransactionReceipt{
		TransactionHash:         types.Hash(receipt.TransactionHash.Hex()),
		Index:                   hexutil.Uint64(receipt.TransactionIndex.ToInt().Uint64()),
		BlockHash:               types.Hash(receipt.BlockHash.Hex()),
		EpochNumber:             types.NewUint64(receipt.BlockNumber.ToInt().Uint64()),
		From:                    api.convertAddress(*receipt.From), // From should always be available
		To:                      api.convertAddressNullable(receipt.To),
		GasUsed:                 receipt.GasUsed,
		GasFee:                  HexBig0,
		ContractCreated:         api.convertAddressNullable(receipt.ContractAddress),
		Logs:                    logs,
		LogsBloom:               types.Bloom(hexutil.Encode(receipt.LogsBloom.Bytes())),
		StateRoot:               *api.convertHashNullable(receipt.StateRoot), // StateRoot should always be available
		OutcomeStatus:           outcomeStatus,
		TxExecErrorMsg:          errMsg,
		GasCoveredBySponsor:     false,
		StorageCoveredBySponsor: false,
		StorageCollateralized:   0,
		StorageReleased:         nil,
	}
}
