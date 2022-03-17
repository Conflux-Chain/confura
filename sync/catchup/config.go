package catchup

type config struct {
	// list of Conflux fullnodes to accelerate catching up until the latest stable epoch
	CfxPool []string
	// threshold for num of db rows per batch persistence
	DbRowsThreshold int `default:"1000"`
	// threshold for num of epochs per batch persistence
	EpochsThreshold int `default:"5000"`
	// capacity of channel per worker to buffer queried epoch data
	WorkerChanSize int `default:"5"`
}
