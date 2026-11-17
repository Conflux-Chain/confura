package catchup

import (
	"fmt"
	"time"
)

type config struct {
	// Pool of full nodes to accelerate catching up until the latest stable epoch
	NodePool struct {
		Cfx []string // for core space
		Eth []string // for evm space
	}
	// threshold for num of db rows per batch persistence
	DbRowsThreshold int `default:"2500"`
	// max number of db rows collected before persistence
	MaxDbRows int `default:"7500"`
	// capacity of channel per worker to buffer queried epoch data
	WorkerChanSize int `default:"5"`
	// benchmark mode
	Benchmark bool
	// boost mode
	Boost boostConfig
}

// boostConfig holds the configuration parameters for boost catch-up mode.
type boostConfig struct {
	// enable boost mode
	Enabled bool
	// task queue sizes to schedule tasks
	TaskQueueSize int `default:"1000"`
	// task result queue sizes to collect results
	ResultQueueSize int `default:"1000"`
	// size of buffer for storage write
	WriteBufferSize int `default:"3000"`
	// default task size and bounds
	DefaultTaskSize uint64 `default:"100"`
	MinTaskSize     uint64 `default:"1"`
	MaxTaskSize     uint64 `default:"1000"`
	// task size adjustment ratios
	IncrementRatio float64 `default:"0.2"`
	DecrementRatio float64 `default:"0.5"`
	// maximum sample size for dynamic new task size estimation
	MaxSampleSize int `default:"20"`
	// max allowed memory usage (in bytes) to trigger backpressure
	MemoryThreshold uint64
	// Memory check interval
	MemoryCheckInterval time.Duration `default:"20s"`
	// Force persistence interval
	ForcePersistenceInterval time.Duration `default:"45s"`
}

func (c config) validate() error {
	if c.WorkerChanSize < 0 || c.DbRowsThreshold <= 0 || c.MaxDbRows < 0 {
		return fmt.Errorf("invalid catch-up buffer or batch size")
	}
	b := c.Boost
	if b.Enabled && (b.TaskQueueSize < 0 || b.ResultQueueSize < 0 || b.WriteBufferSize < 0 || b.DefaultTaskSize == 0 || b.MinTaskSize == 0 || b.MaxTaskSize < b.MinTaskSize || b.MaxSampleSize <= 0 || b.ForcePersistenceInterval <= 0 || b.MemoryCheckInterval <= 0) {
		return fmt.Errorf("invalid boost limits or intervals")
	}
	return nil
}
