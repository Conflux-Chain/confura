package catchup

type config struct {
	// list of Conflux fullnodes to accelerate catching up until the latest stable epoch
	CfxPool []string
	// threshold for num of db rows per batch persistence
	DbRowsThreshold int `default:"1500"`
	// capacity of channel per worker to buffer queried epoch data
	WorkerChanSize int `default:"10"`
}
