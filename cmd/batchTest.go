package cmd

import (
	"fmt"
	"github.com/Conflux-Chain/confura/util/rpc"
	sdk "github.com/Conflux-Chain/go-conflux-sdk"
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
	cfxClient = sdk.MustNewClient(cfxClient.GetNodeURL(), sdk.ClientOption{
		//Logger: os.Stderr,
	})

	workerCount, _ := cmd.Flags().GetInt("worker")
	round, _ := cmd.Flags().GetInt("round")
	epoch, _ := cmd.Flags().GetUint64("epoch")
	v, e := cfxClient.GetEpochReceipts(*types.NewEpochOrBlockHashWithEpoch(types.NewEpochNumberUint64(0)))
	logrus.WithError(e).Info("epoch 0 receipts", v)

	requestFn = func(ep uint64) (*EpochResult, error) {
		v, e := cfxClient.GetEpochReceipts(*types.NewEpochOrBlockHashWithEpoch(types.NewEpochNumberUint64(ep)))
		if e != nil {
			logrus.WithError(e).Error("normal rpc failed")
		} else {
			//logrus.Info("result is ", v)
		}
		ret := &EpochResult{}
		buildEpochResult(v, ret)
		return ret, e
	}

	start := time.Now()
	doTest(workerCount, round, epoch, false)
	elapsed := time.Since(start)
	log.Printf("threads   mod, took %s , %s per request ", elapsed, elapsed/time.Duration(round))

	reqArr := make([]rpc2.BatchElem, workerCount)
	requestFn = func(seq uint64) (*EpochResult, error) {
		logrus.Debug(" batch sequence ", seq)
		base := int(seq)*workerCount + int(epoch)
		for e := 0; e < workerCount; e++ {
			var receipts [][]types.TransactionReceipt
			wantE := base + e
			logrus.Debug("batch element , epoch ", wantE, " base ", base)
			args := make([]interface{}, 1)
			args[0] = types.NewEpochNumberUint64(uint64(wantE))
			reqArr[e] = rpc2.BatchElem{
				Method: "cfx_getEpochReceipts",
				Args:   args,
				Result: &receipts,
			}
		}
		be := cfxClient.BatchCallRPC(reqArr)
		if be != nil {
			logrus.WithError(be).Error("batch call rpc failed.")
			return nil, be
		}
		retAll := &EpochResult{}
		for _, req := range reqArr {
			if req.Error != nil {
				logrus.WithError(req.Error).WithFields(logrus.Fields{
					"method": req.Method, "args": req.Args, "result": req.Result,
				}).Error("batch call rpc failed, in element.")
				return nil, req.Error
			} else {
				ret := &EpochResult{}
				bi, _ := req.Args[0].(*types.Epoch).ToInt()
				logrus.Debug("epoch ", bi.Uint64(), " receipts ", req.Result)
				buildEpochResult(*(req.Result.(*[][]types.TransactionReceipt)), ret)
				retAll.txCount += ret.txCount
			}
		}
		return retAll, nil
	}
	start = time.Now()
	doTest(workerCount, round/workerCount, 0, false)
	elapsed = time.Since(start)
	log.Printf("batch RPC mod, took %s , %s per request ", elapsed, elapsed/time.Duration(round))
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
