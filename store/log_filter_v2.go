package store

import (
	"github.com/Conflux-Chain/go-conflux-sdk/types"
	"github.com/Conflux-Chain/go-conflux-sdk/types/cfxaddress"
	web3goTypes "github.com/openweb3/web3go/types"
)

type LogFilterV2 struct {
	BlockFrom uint64
	BlockTo   uint64
	Contracts VariadicValue
	Topics    []VariadicValue // event hash and indexed data 1, 2, 3
}

func ParseCfxLogFilter(blockFrom, blockTo uint64, filter *types.LogFilter) LogFilterV2 {
	var vvs []VariadicValue

	for _, hashes := range filter.Topics {
		vvs = append(vvs, newVariadicValueByHashes(hashes))
	}

	return LogFilterV2{
		BlockFrom: blockFrom,
		BlockTo:   blockTo,
		Contracts: newVariadicValueByAddress(filter.Address),
		Topics:    vvs,
	}
}

func ParseEthLogFilterV2(blockFrom, blockTo uint64, filter *web3goTypes.FilterQuery, networkId uint32) LogFilterV2 {
	var contracts []string
	for i := range filter.Addresses {
		// convert eth hex40 address to cfx base32 address
		addr, _ := cfxaddress.NewFromCommon(filter.Addresses[i], networkId)
		contracts = append(contracts, addr.MustGetBase32Address())
	}

	var vvs []VariadicValue
	for _, topic := range filter.Topics {
		var hashes []string
		for _, hash := range topic {
			hashes = append(hashes, hash.Hex())
		}
		vvs = append(vvs, NewVariadicValue(hashes...))
	}

	return LogFilterV2{
		BlockFrom: blockFrom,
		BlockTo:   blockTo,
		Contracts: NewVariadicValue(contracts...),
		Topics:    vvs,
	}
}
