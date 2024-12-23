package catchup

type config struct {
	// list of Conflux fullnodes to accelerate catching up until the latest stable epoch
	CfxPool []string
	// threshold for num of db rows per batch persistence
	DbRowsThreshold int `default:"2500"`
	// max number of db rows collected before persistence
	MaxDbRows int `default:"7500"`
	// capacity of channel per worker to buffer queried epoch data
	WorkerChanSize int `default:"5"`
	// max allowed memory usage (in bytes)
	MemoryThreshold uint64
	// benchmark mode
	Benchmark bool
}
