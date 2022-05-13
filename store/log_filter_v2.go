package store

type LogFilterV2 struct {
	BlockFrom uint64
	BlockTo   uint64
	Contracts VariadicValue
	Topics    []VariadicValue // event hash and indexed data 1, 2, 3
}
