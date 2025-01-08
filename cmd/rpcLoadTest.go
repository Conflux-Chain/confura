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
	results := make(chan uint64, round)

	for w := 1; w <= workerCount; w++ {
		go worker(w, jobs, results)
	}

	for j := 0; j < round; j++ {
		jobs <- uint64(epoch)
		if !sameEpoch {
			epoch += 1
		}
	}
	close(jobs)

	for a := 1; a <= round; a++ {
		<-results
	}

	elapsed := time.Since(start)
	log.Printf("it tooks %s , average %s per epoch", elapsed, elapsed/time.Duration(round))

	logrus.Info("done")
}

func worker(id int, jobs <-chan uint64, results chan<- uint64) {
	for j := range jobs {
		e := doRequest(j)
		if e != nil {
			logrus.WithError(e).Errorf("worker %d , error for epoch %d\n", id, j)
			break
		}
		results <- j
	}
}

func doRequest(epoch uint64) error {
	arr, e := cfxClient.GetBlocksByEpoch(types.NewEpochNumberUint64(epoch))
	if e != nil {
		return e
	}
	p := arr[len(arr)-1]
	for _, h := range arr {
		_, e = cfxClient.GetBlockByHashWithPivotAssumption(h, p, hexutil.Uint64(epoch))
		if e != nil {
			return e
		}
		_, e = cfxClient.GetBlockTraces(h)
		if e != nil {
			return e
		}
	}
	_, e = cfxClient.GetEpochReceipts(*types.NewEpochOrBlockHashWithEpoch(types.NewEpochNumberUint64(epoch)), false)
	return e
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
