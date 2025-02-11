package cmd

import (
	"fmt"
	"github.com/Conflux-Chain/confura/util/rpc"
	sdk "github.com/Conflux-Chain/go-conflux-sdk"
	"github.com/Conflux-Chain/go-conflux-sdk/types"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"log"
	"time"
)

// rpcLoadTestCmd represents the rpcLoadTest command
var rpcLoadTestCmd = &cobra.Command{
	Use:   "rpcLoadTest",
	Short: "A brief description of your command",
	Long:  ``,
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("rpcLoadTest called")
		workerCount, _ := cmd.Flags().GetInt("worker")
		round, _ := cmd.Flags().GetInt("round")
		epoch, _ := cmd.Flags().GetUint64("epoch")
		sameEpoch, _ := cmd.Flags().GetBool("sameEpoch")
		doTest(workerCount, round, epoch, sameEpoch)
	},
}

var cfxClient *sdk.Client

func doTest(workerCount int, round int, epoch uint64, sameEpoch bool) {
	cfx := rpc.MustNewCfxClientsFromViper()
	cfxClient = cfx[0]

	logrus.Infof("worker %d round %d from epoch %d same epoch %t\n", workerCount, round, epoch, sameEpoch)
	start := time.Now()

	jobs := make(chan uint64, round)
	results := make(chan *EpochResult, round)

	for w := 1; w <= workerCount; w++ {
		go worker(w, jobs, results)
	}

	for j := 0; j < round; j++ {
		jobs <- epoch
		if !sameEpoch {
			epoch += 1
		}
	}
	close(jobs)

	totalInfo := &EpochResult{}
	for a := 1; a <= round; a++ {
		ret := <-results
		if ret != nil {
			totalInfo.blockCount += ret.blockCount
			totalInfo.txCount += ret.txCount
			totalInfo.eventCount += ret.eventCount
			totalInfo.traceCount += ret.traceCount
		}
	}

	elapsed := time.Since(start)
	if showSummary {
		log.Printf("it took %s , average %s per epoch "+
			"block %d TX %d event %d trace %d", elapsed, elapsed/time.Duration(round),
			totalInfo.blockCount, totalInfo.txCount, totalInfo.eventCount, totalInfo.traceCount)
	}

}

type EpochResult struct {
	blockCount int
	txCount    int
	traceCount int
	eventCount int
}

var requestFn = doRequest
var showSummary = true

func worker(id int, jobs <-chan uint64, results chan<- *EpochResult) {
	for j := range jobs {
		ret, e := requestFn(j)
		if e != nil {
			logrus.WithError(e).Errorf("worker %d , error for epoch %d\n", id, j)
			break
		}
		results <- ret
	}
}

func doRequest(epoch uint64) (*EpochResult, error) {
	ret := &EpochResult{}
	arr, e := cfxClient.GetBlocksByEpoch(types.NewEpochNumberUint64(epoch))
	if e != nil {
		return nil, e
	}
	ret.blockCount += len(arr)
	p := arr[len(arr)-1]
	for _, h := range arr {
		_, e = cfxClient.GetBlockByHashWithPivotAssumption(h, p, hexutil.Uint64(epoch))
		if e != nil {
			return nil, e
		}
		blkTrace, e := cfxClient.GetBlockTraces(h)
		if e != nil {
			return nil, e
		}
		for _, tt := range blkTrace.TransactionTraces {
			ret.traceCount += len(tt.Traces)
		}
	}
	receipts, e := cfxClient.GetEpochReceiptsByPivotBlockHash(p)
	if e != nil {
		return nil, e
	}
	for _, rr2 := range receipts {
		for _, r := range rr2 {
			if r.OutcomeStatus == 1 || r.OutcomeStatus == 0 {
				ret.txCount++
				ret.eventCount += len(r.Logs)
			}
		}
	}

	return ret, e
}

func init() {
	rootCmd.AddCommand(rpcLoadTestCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// rpcLoadTestCmd.PersistentFlags().String("foo", "", "A help for foo")
	rpcLoadTestCmd.Flags().IntP("worker", "w", 1, "worker count")
	rpcLoadTestCmd.Flags().IntP("round", "r", 1, "round")
	rpcLoadTestCmd.Flags().Uint64P("epoch", "e", uint64(1), "start from epoch")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	rpcLoadTestCmd.Flags().BoolP("sameEpoch", "s", false, "use same epoch")
}
