package store

import (
	"github.com/Conflux-Chain/go-conflux-sdk/types"
)

type LogFilterV2 struct {
	BlockFrom uint64
	BlockTo   uint64
	Contracts VariadicValue
	Topics    []VariadicValue // event hash and indexed data 1, 2, 3
}

func ParseCfxLogFilter(blockFrom, blockTo uint64, cfxFilter *types.LogFilter) LogFilterV2 {
	var vvs []VariadicValue

	for _, hashes := range cfxFilter.Topics {
		vvs = append(vvs, newVariadicValueByHashes(hashes))
	}

	return LogFilterV2{
		BlockFrom: blockFrom,
		BlockTo:   blockTo,
		Contracts: newVariadicValueByAddress(cfxFilter.Address),
		Topics:    vvs,
	}
}
