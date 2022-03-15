package catchup

import (
	"fmt"
	"time"

	gmetrics "github.com/ethereum/go-ethereum/metrics"
	"github.com/sirupsen/logrus"
)

type benchmarker struct {
	startTime, endTime   time.Time
	persistDbRowsMetrics gmetrics.Histogram
	persistTimer         gmetrics.Timer
	fetchPerEpochTimer   gmetrics.Timer
}

func newBenchmarker() *benchmarker {
	return &benchmarker{
		persistDbRowsMetrics: gmetrics.NewHistogram(gmetrics.NewExpDecaySample(1024, 0.015)),
		persistTimer:         gmetrics.NewTimer(),
		fetchPerEpochTimer:   gmetrics.NewTimer(),
	}
}

func (b *benchmarker) markStart() {
	b.startTime = time.Now()
}

func (b *benchmarker) report(start, end uint64) {
	b.endTime = time.Now()
	defer b.persistDbRowsMetrics.Clear()

	totalDuration := b.endTime.Sub(b.startTime).Milliseconds()
	totalEpochs := end - start

	logrus.WithFields(logrus.Fields{
		"startTime": b.startTime, "endTime": b.endTime,
		"startEpoch": start, "endEpoch": end,
	}).Info("Catch up perf benchmark reports generated")

	fmt.Println("// -------------- summary ---------------")
	fmt.Printf("   total epochs: %v\n", totalEpochs)
	fmt.Printf("total durations: %v(ms)\n", totalDuration)

	if totalEpochs == 0 {
		return
	}

	avgDurationPerEpoch := float64(totalDuration) / float64(totalEpochs)
	fmt.Printf("avg duration per epoch: %.2f(ms)\n", avgDurationPerEpoch)

	fmt.Println("// -------- persisted db rows -----------")
	fmt.Printf("     total db rows: %v\n", b.persistDbRowsMetrics.Sum())
	fmt.Printf(" max batch db rows: %v\n", b.persistDbRowsMetrics.Max())
	fmt.Printf(" min batch db rows: %v\n", b.persistDbRowsMetrics.Min())
	fmt.Printf("mean batch db rows: %v\n", b.persistDbRowsMetrics.Mean())
	fmt.Printf(" p99 batch db rows: %v\n", b.persistDbRowsMetrics.Percentile(99))
	fmt.Printf(" p75 batch db rows: %v\n", b.persistDbRowsMetrics.Percentile(75))

	fmt.Println("// ------ persisted db durations --------")
	fmt.Printf(" max duration: %.2f(ms)\n", float64(b.persistTimer.Max())/1e6)
	fmt.Printf(" min duration: %.2f(ms)\n", float64(b.persistTimer.Min())/1e6)
	fmt.Printf("mean duration: %.2f(ms)\n", b.persistTimer.Mean()/1e6)
	fmt.Printf(" p99 duration: %.2f(ms)\n", float64(b.persistTimer.Percentile(99))/1e6)
	fmt.Printf(" p75 duration: %.2f(ms)\n", float64(b.persistTimer.Percentile(75))/1e6)

	fmt.Println("// --------- persisted db tps -----------")
	fmt.Printf("mean tps: %v\n", b.persistTimer.RateMean())
	fmt.Printf("  m1 tps: %v\n", b.persistTimer.Rate1())
	fmt.Printf("  m5 tps: %v\n", b.persistTimer.Rate5())
	fmt.Printf(" m15 tps: %v\n", b.persistTimer.Rate15())

	fmt.Println("// ------- epoch fetch duration ---------")
	fmt.Printf("  total epochs: %v\n", b.fetchPerEpochTimer.Count())
	fmt.Printf("  max duration: %.2f(ms)\n", float64(b.fetchPerEpochTimer.Max())/1e6)
	fmt.Printf("  min duration: %.2f(ms)\n", float64(b.fetchPerEpochTimer.Min()/1e6))
	fmt.Printf(" mean duration: %.2f(ms)\n", b.fetchPerEpochTimer.Mean()/1e6)
	fmt.Printf("  p99 duration: %.2f(ms)\n", float64(b.fetchPerEpochTimer.Percentile(99))/1e6)
	fmt.Printf("  p75 duration: %.2f(ms)\n", float64(b.fetchPerEpochTimer.Percentile(75))/1e6)

	fmt.Println("// ---------- epoch fetch tps -----------")
	fmt.Printf("mean tps: %v\n", b.fetchPerEpochTimer.RateMean())
	fmt.Printf("  m1 tps: %v\n", b.fetchPerEpochTimer.Rate1())
	fmt.Printf("  m5 tps: %v\n", b.fetchPerEpochTimer.Rate5())
	fmt.Printf(" m15 tps: %v\n", b.fetchPerEpochTimer.Rate15())
}

func (b *benchmarker) metricPersistDbRows(dbRows int64) {
	b.persistDbRowsMetrics.Update(dbRows)
}

func (b *benchmarker) metricPersistDuration(start time.Time) {
	b.persistTimer.UpdateSince(start)
}

func (b *benchmarker) metricFetchPerEpochDuration(start time.Time) {
	b.fetchPerEpochTimer.UpdateSince(start)
}
