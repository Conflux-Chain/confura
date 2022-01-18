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

	// TODO missed fields: ContractCreated, Status
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

	return &types.BlockHeader{
		Hash:                  types.Hash(block.Hash.Hex()),
		ParentHash:            types.Hash(block.ParentHash.Hex()),
		Height:                block.Number,
		Miner:                 api.convertAddress(block.Miner),
		DeferredStateRoot:     types.Hash(block.StateRoot.Hex()),
		DeferredReceiptsRoot:  types.Hash(block.ReceiptsRoot.Hex()),
		DeferredLogsBloomHash: types.Hash(hexutil.Encode(block.LogsBloom.Bytes())),
		Blame:                 0,
		TransactionsRoot:      types.Hash(block.TransactionsRoot.Hex()),
		EpochNumber:           block.Number,
		BlockNumber:           block.Number,
		GasLimit:              types.NewBigInt(uint64(block.GasLimit)),
		GasUsed:               types.NewBigInt(uint64(block.GasUsed)),
		Timestamp:             types.NewBigInt(uint64(block.Timestamp)),
		Difficulty:            block.Difficulty,
		PowQuality:            HexBig0,
		RefereeHashes:         referees,
		Adaptive:              false,
		Nonce:                 types.NewBigInt(block.Nonce.Uint64()),
		Size:                  types.NewBigInt(uint64(block.Size)),
		Custom:                custom,
	}
}

func (api *CfxAPI) convertBlock(block *ethTypes.Block) *types.Block {
	if block == nil {
		return nil
	}

	txs := make([]types.Transaction, len(block.Transactions.Transactions))
	for i := range block.Transactions.Transactions {
		txs[i] = *api.convertTx(&block.Transactions.Transactions[i])
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

	txs := make([]types.Hash, len(block.Transactions.Hashes))
	for i := range block.Transactions.Hashes {
		txs[i] = types.Hash(block.Transactions.Hashes[i].Hex())
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
		EpochNumber:         types.NewBigInt(uint64(log.BlockNumber)),
		TransactionHash:     api.convertHashNullable(&log.TxHash),
		TransactionIndex:    types.NewBigInt(uint64(log.TxIndex)), // tx index in block
		LogIndex:            types.NewBigInt(uint64(log.Index)),   // log index in block
		TransactionLogIndex: log.TransactionLogIndex,              // log index in tx
	}
}

func (api *CfxAPI) convertReceipt(receipt *ethTypes.Receipt) *types.TransactionReceipt {
	if receipt == nil {
		return nil
	}

	logs := make([]types.Log, len(receipt.Logs))
	for i := range receipt.Logs {
		logs[i] = *api.convertLog(receipt.Logs[i])
	}

	var stateRoot types.Hash
	if receipt.Root == nil || len(*receipt.Root) == 0 {
		stateRoot = types.Hash(common.Hash{}.Hex())
	} else {
		stateRoot = types.Hash(receipt.Root.String())
	}

	// StatusCode should always be available
	var outcomeStatus hexutil.Uint64
	var errMsg *string
	if *receipt.Status == hexutil.Uint(gethTypes.ReceiptStatusFailed) {
		outcomeStatus = 1
		errMsg = &defaultReceiptErrMsg
	}

	// TODO missed fields: GasFee (price * gasCharged?), TxExecErrorMsg
	return &types.TransactionReceipt{
		TransactionHash:         types.Hash(receipt.TransactionHash.Hex()),
		Index:                   receipt.TransactionIndex,
		BlockHash:               types.Hash(receipt.BlockHash.Hex()),
		EpochNumber:             &receipt.BlockNumber,
		From:                    api.convertAddress(receipt.From),
		To:                      api.convertAddressNullable(receipt.To),
		GasUsed:                 types.NewBigInt(uint64(receipt.GasUsed)),
		GasFee:                  HexBig0,
		ContractCreated:         api.convertAddressNullable(receipt.ContractAddress),
		Logs:                    logs,
		LogsBloom:               types.Bloom(hexutil.Encode(receipt.LogsBloom.Bytes())),
		StateRoot:               stateRoot,
		OutcomeStatus:           outcomeStatus,
		TxExecErrorMsg:          errMsg,
		GasCoveredBySponsor:     false,
		StorageCoveredBySponsor: false,
		StorageCollateralized:   0,
		StorageReleased:         nil,
	}
}
