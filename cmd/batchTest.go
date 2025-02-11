package cmd

import (
	"fmt"
	"github.com/Conflux-Chain/confura/util/rpc"
	"github.com/Conflux-Chain/go-conflux-sdk/types"
	rpc2 "github.com/openweb3/go-rpc-provider"
	"github.com/sirupsen/logrus"
	"log"
	"time"

	"github.com/spf13/cobra"
)

// batchTestCmd represents the batchTest command
var batchTestCmd = &cobra.Command{
	Use:   "batchTest",
	Short: "A brief description of your command",
	Long:  ``,
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("batchTest called")
		doBatchTest(cmd)
	},
}

func doBatchTest(cmd *cobra.Command) {
	cfx := rpc.MustNewCfxClientsFromViper()
	cfxClient = cfx[0]
	logrus.Info("RPC at ", cfxClient.GetNodeURL())

	workerCount, _ := cmd.Flags().GetInt("worker")
	round, _ := cmd.Flags().GetInt("round")
	epoch, _ := cmd.Flags().GetUint64("epoch")

	requestFn = func(ep uint64) (*EpochResult, error) {
		_, e := cfxClient.GetEpochReceipts(*types.NewEpochOrBlockHashWithEpoch(types.NewEpochNumberUint64(ep)))
		if e != nil {
			logrus.WithError(e).Error("normal rpc failed")
		}
		return nil, e
	}
	start := time.Now()
	doTest(workerCount, round, epoch, false)
	elapsed := time.Since(start)
	log.Printf("threads   mod, took %s , average %s per request ", elapsed, elapsed/time.Duration(round))

	start = time.Now()
	reqArr := make([]rpc2.BatchElem, workerCount)
	for r := 0; r < round; r += workerCount {
		for e := 0; e < workerCount; e++ {
			wantE := int(epoch) + r + e
			args := make([]interface{}, 1)
			args[0] = types.NewEpochNumberUint64(uint64(wantE))
			reqArr[e] = rpc2.BatchElem{
				Method: "cfx_getEpochReceipts",
				Args:   args,
			}
		}
		be := cfxClient.BatchCallRPC(reqArr)
		if be != nil {
			logrus.WithError(be).Error("batch call rpc failed.")
			break
		}
		stop := false
		for _, req := range reqArr {
			if req.Error != nil {
				logrus.WithError(req.Error).WithFields(logrus.Fields{
					"method": req.Method, "args": req.Args, "result": req.Result,
				}).Error("batch call rpc failed, in element.")
				stop = true
			}
		}
		if stop {
			break
		}
	}
	elapsed = time.Since(start)
	log.Printf("batch RPC mod, took %s , average %s per request ", elapsed, elapsed/time.Duration(round))
}

func init() {
	rootCmd.AddCommand(batchTestCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// batchTestCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// batchTestCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")

	batchTestCmd.Flags().IntP("round", "r", 1, "round")
	batchTestCmd.Flags().Uint64P("epoch", "e", uint64(1), "start from epoch")
	batchTestCmd.Flags().IntP("worker", "w", 1, "worker count")
}
