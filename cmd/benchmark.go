package cmd

import (
	"context"
	"fmt"
	"sync"
	"time"

	cmdutil "github.com/Conflux-Chain/confura/cmd/util"
	"github.com/Conflux-Chain/confura/util"
	"github.com/Conflux-Chain/confura/util/metrics"
	"github.com/Conflux-Chain/confura/util/rpc"
	sdk "github.com/Conflux-Chain/go-conflux-sdk"
	gethmetrics "github.com/ethereum/go-ethereum/metrics"
	"github.com/openweb3/go-rpc-provider/utils"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

var (
	// benchmarking options
	benchOpts struct {
		nodeURL string
	}

	benchmarkCmd = &cobra.Command{
		Use:   "benchmark",
		Short: "Full node availability benchmarking",
		Run:   startBenchmarking,
	}
)

func init() {
	benchmarkCmd.Flags().StringVar(
		&benchOpts.nodeURL, "cfx", "", "Benchmarking full node URL",
	)
	benchmarkCmd.MarkFlagRequired("cfx")

	rootCmd.AddCommand(benchmarkCmd)
}

func startBenchmarking(*cobra.Command, []string) {
	var cfxs [2]*sdk.Client
	for i := 0; i < len(cfxs); i++ {
		cfxs[i] = rpc.MustNewCfxClient(
			benchOpts.nodeURL,
			rpc.WithClientRetryCount(0),
			rpc.WithClientRequestTimeout(1*time.Second),
		)

		defer cfxs[i].Close()
	}

	wg := &sync.WaitGroup{}
	ctx, cancel := context.WithCancel(context.Background())

	wg.Add(1)
	mc1 := newMetricsCollector(benchOpts.nodeURL, "GetEpochNumber")
	go doBenchmark(ctx, wg, mc1, func() error {
		_, err := cfxs[0].GetEpochNumber()
		return err
	})

	wg.Add(1)
	mc2 := newMetricsCollector(benchOpts.nodeURL, "GetClientVersion")
	go doBenchmark(ctx, wg, mc2, func() error {
		_, err := cfxs[1].GetClientVersion()
		return err
	})

	cmdutil.GracefulShutdown(wg, cancel)
}

func doBenchmark(
	ctx context.Context, wg *sync.WaitGroup, mc *metricsCollector, dowork func() error) {
	defer wg.Done()

	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()

	timer := time.NewTimer(time.Second)
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			startTime := time.Now()
			err := dowork()
			mc.update(err, startTime)

			timer.Reset(time.Second)
		case <-ticker.C:
			mc.dump()
		}
	}
}

type metricsCollector struct {
	metricsKeyPrefix    string
	successPercentage   metrics.Percentage
	rpcErrPercentage    metrics.Percentage
	nonRpcErrPercentage metrics.Percentage
	durationTimer       gethmetrics.Timer
}

func newMetricsCollector(nodeURL, method string) *metricsCollector {
	host := rpc.Url2NodeName(nodeURL)
	metricsKeyPrefix := fmt.Sprintf("infura/benchmark/host/%v/method/%v", host, method)

	successPercentMk := fmt.Sprintf("%v/rate/success", metricsKeyPrefix)
	successPercentage := metrics.GetOrRegisterTimeWindowPercentageDefault(successPercentMk)

	rpcErrPercentMk := fmt.Sprintf("%v/rate/rpcErr", metricsKeyPrefix)
	rpcErrPercentage := metrics.GetOrRegisterTimeWindowPercentageDefault(rpcErrPercentMk)

	nonRpcErrPercentMk := fmt.Sprintf("%v/rate/nonRpcErr", metricsKeyPrefix)
	nonRpcErrPercentage := metrics.GetOrRegisterTimeWindowPercentageDefault(nonRpcErrPercentMk)

	durationMk := fmt.Sprintf("%v/duration", metricsKeyPrefix)
	durationTimer := metrics.GetOrRegisterTimer(durationMk)

	return &metricsCollector{
		metricsKeyPrefix:    metricsKeyPrefix,
		successPercentage:   successPercentage,
		rpcErrPercentage:    rpcErrPercentage,
		nonRpcErrPercentage: nonRpcErrPercentage,
		durationTimer:       durationTimer,
	}
}

func (mc *metricsCollector) dump() {
	logrus.WithFields(logrus.Fields{
		"metricsKeyPrefix":    mc.metricsKeyPrefix,
		"successPercentage":   mc.successPercentage.Value(),
		"rpcErrPercentage":    mc.rpcErrPercentage.Value(),
		"nonRpcErrPercentage": mc.nonRpcErrPercentage.Value(),
		"rate1Min":            fmt.Sprintf("%.2f", mc.durationTimer.Rate1()),
		"rate5Min":            fmt.Sprintf("%.2f", mc.durationTimer.Rate5()),
		"rate15Min":           fmt.Sprintf("%.2f", mc.durationTimer.Rate15()),
		"latencyMean":         fmt.Sprintf("%.2f(ms)", mc.durationTimer.Mean()/1e6),
		"latencyP50":          fmt.Sprintf("%.2f(ms)", mc.durationTimer.Percentile(0.5)/1e6),
		"latencyP75":          fmt.Sprintf("%.2f(ms)", mc.durationTimer.Percentile(0.75)/1e6),
		"latencyP90":          fmt.Sprintf("%.2f(ms)", mc.durationTimer.Percentile(0.9)/1e6),
		"latencyP95":          fmt.Sprintf("%.2f(ms)", mc.durationTimer.Percentile(0.95)/1e6),
		"latencyP99":          fmt.Sprintf("%.2f(ms)", mc.durationTimer.Percentile(0.99)/1e6),
	}).Info("Metrics dumpped periodically")
}

func (mc *metricsCollector) update(err error, start time.Time) {
	var isNilErr, isRpcErr bool
	if isNilErr = util.IsInterfaceValNil(err); !isNilErr {
		isRpcErr = utils.IsRPCJSONError(err)
	}

	// Percentage statistics
	mc.successPercentage.Mark(isNilErr)
	mc.rpcErrPercentage.Mark(isRpcErr)
	mc.nonRpcErrPercentage.Mark(!isNilErr && !isRpcErr)

	// Only update QPS & Latency if success or rpc error. Because, io error usually takes long time
	// and will contaminate the average latency.
	if isNilErr || isRpcErr {
		mc.durationTimer.UpdateSince(start)
	}
}
