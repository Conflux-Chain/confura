package catchup

import (
	"fmt"
	"sync/atomic"
	"time"

	gmetrics "github.com/ethereum/go-ethereum/metrics"
	"github.com/sirupsen/logrus"
)

type benchmarker struct {
	startTime, endTime   time.Time
	persistDbRowsMetrics gmetrics.Histogram
	persistEpochsMetrics gmetrics.Histogram
	persistTimer         gmetrics.Timer
	fetchPerEpochTimer   gmetrics.Timer

	avgPersistDurationPerDbRowMetrics gmetrics.Histogram
	avgPersistDurationPerEpochMetrics gmetrics.Histogram

	totalPersistDuration time.Duration
	totalPersistDbRows   int64
	totalPersistEpochs   int64

	totalFetchEpochs   int64
	totalFetchDuration time.Duration
}

func newBenchmarker() *benchmarker {
	return &benchmarker{
		persistDbRowsMetrics: gmetrics.NewHistogram(gmetrics.NewExpDecaySample(1024, 0.015)),
		persistEpochsMetrics: gmetrics.NewHistogram(gmetrics.NewExpDecaySample(1024, 0.015)),

		avgPersistDurationPerDbRowMetrics: gmetrics.NewHistogram(gmetrics.NewExpDecaySample(1024, 0.015)),
		avgPersistDurationPerEpochMetrics: gmetrics.NewHistogram(gmetrics.NewExpDecaySample(1024, 0.015)),

		persistTimer:       gmetrics.NewTimer(),
		fetchPerEpochTimer: gmetrics.NewTimer(),
	}
}

func (b *benchmarker) markStart() {
	b.startTime = time.Now()
}

func (b *benchmarker) report(start, end uint64) {
	b.endTime = time.Now()
	defer func() {
		b.persistDbRowsMetrics.Clear()
		b.persistEpochsMetrics.Clear()

		b.avgPersistDurationPerDbRowMetrics.Clear()
		b.avgPersistDurationPerEpochMetrics.Clear()
	}()

	totalDuration := b.endTime.Sub(b.startTime)
	totalEpochs := end - start

	totalPersistDuration := atomic.LoadInt64((*int64)(&b.totalPersistDuration))
	totalPersistDbRows := atomic.LoadInt64(&b.totalPersistDbRows)
	totalPersistEpochs := atomic.LoadInt64(&b.totalPersistEpochs)

	totalFetchDuration := atomic.LoadInt64((*int64)(&b.totalFetchDuration))
	totalFetchEpochs := atomic.LoadInt64(&b.totalFetchEpochs)

	logrus.WithFields(logrus.Fields{
		"startTime": b.startTime, "endTime": b.endTime,
		"startEpoch": start, "endEpoch": end,
	}).Info("Catch up perf benchmark reports generated")

	fmt.Println("// ----------------- summary ------------------")
	fmt.Printf("   total epochs: %v\n", totalEpochs)
	fmt.Printf("total durations: %.2f(ms)\n\n", float64(totalDuration)/1e6)

	fmt.Printf("  total persist epochs: %v\n", totalPersistEpochs)
	fmt.Printf(" total persist db rows: %v\n", totalPersistDbRows)
	fmt.Printf("total persist duration: %.2f(ms)\n\n", float64(totalPersistDuration)/1e6)

	fmt.Printf("   total fetch epochs: %v\n", totalFetchEpochs)
	fmt.Printf("total fetch durations: %.2f(ms)\n\n", float64(totalFetchDuration)/1e6)

	if totalEpochs == 0 {
		return
	}

	fmt.Printf("         avg duration/epoch: %.2f(ms)\n", float64(totalDuration)/float64(totalEpochs)/1e6)
	fmt.Printf("   avg fetch duration/epoch: %.2f(ms)\n", float64(totalFetchDuration)/float64(totalFetchEpochs)/1e6)
	fmt.Printf(" avg persist duration/epoch: %.2f(ms)\n", float64(totalPersistDuration)/float64(totalPersistEpochs)/1e6)
	if totalPersistDbRows > 0 {
		fmt.Printf("avg persist duration/db row: %.2f(ms)\n", float64(totalPersistDuration)/float64(totalPersistDbRows)/1e6)
	}

	fmt.Println("// -------- batch persisted db rows -----------")
	fmt.Printf("     total db rows: %v\n", b.persistDbRowsMetrics.Sum())
	fmt.Printf(" max batch db rows: %v\n", b.persistDbRowsMetrics.Max())
	fmt.Printf(" min batch db rows: %v\n", b.persistDbRowsMetrics.Min())
	fmt.Printf("mean batch db rows: %v\n", b.persistDbRowsMetrics.Mean())
	fmt.Printf(" p99 batch db rows: %v\n", b.persistDbRowsMetrics.Percentile(99))
	fmt.Printf(" p75 batch db rows: %v\n", b.persistDbRowsMetrics.Percentile(75))

	fmt.Println("// --------- batch persisted epochs -----------")
	fmt.Printf("     total epochs: %v\n", b.persistEpochsMetrics.Sum())
	fmt.Printf(" max batch epochs: %v\n", b.persistEpochsMetrics.Max())
	fmt.Printf(" min batch epochs: %v\n", b.persistEpochsMetrics.Min())
	fmt.Printf("mean batch epochs: %v\n", b.persistEpochsMetrics.Mean())
	fmt.Printf(" p99 batch epochs: %v\n", b.persistEpochsMetrics.Percentile(99))
	fmt.Printf(" p75 batch epochs: %v\n", b.persistEpochsMetrics.Percentile(75))

	fmt.Println("// ------ batch persisted db durations --------")
	fmt.Printf("total duration: %.2f(ms)\n", float64(b.persistTimer.Sum())/1e6)
	fmt.Printf("  max duration: %.2f(ms)\n", float64(b.persistTimer.Max())/1e6)
	fmt.Printf("  min duration: %.2f(ms)\n", float64(b.persistTimer.Min())/1e6)
	fmt.Printf(" mean duration: %.2f(ms)\n", b.persistTimer.Mean()/1e6)
	fmt.Printf("  p99 duration: %.2f(ms)\n", float64(b.persistTimer.Percentile(99))/1e6)
	fmt.Printf("  p75 duration: %.2f(ms)\n", float64(b.persistTimer.Percentile(75))/1e6)

	fmt.Println("// ------ avg persist duration/db row ---------")
	fmt.Printf("total duration: %.2f(ms)\n", float64(b.avgPersistDurationPerDbRowMetrics.Sum())/1e6)
	fmt.Printf("  max duration: %.2f(ms)\n", float64(b.avgPersistDurationPerDbRowMetrics.Max())/1e6)
	fmt.Printf("  min duration: %.2f(ms)\n", float64(b.avgPersistDurationPerDbRowMetrics.Min())/1e6)
	fmt.Printf(" mean duration: %.2f(ms)\n", float64(b.avgPersistDurationPerDbRowMetrics.Mean())/1e6)
	fmt.Printf("  p99 duration: %.2f(ms)\n", float64(b.avgPersistDurationPerDbRowMetrics.Percentile(99))/1e6)
	fmt.Printf("  p75 duration: %.2f(ms)\n", float64(b.avgPersistDurationPerDbRowMetrics.Percentile(75))/1e6)

	fmt.Println("// ------ avg persist duration/epoch ----------")
	fmt.Printf("total duration: %.2f(ms)\n", float64(b.avgPersistDurationPerEpochMetrics.Sum())/1e6)
	fmt.Printf("  max duration: %.2f(ms)\n", float64(b.avgPersistDurationPerEpochMetrics.Max())/1e6)
	fmt.Printf("  min duration: %.2f(ms)\n", float64(b.avgPersistDurationPerEpochMetrics.Min())/1e6)
	fmt.Printf(" mean duration: %.2f(ms)\n", float64(b.avgPersistDurationPerEpochMetrics.Mean())/1e6)
	fmt.Printf("  p99 duration: %.2f(ms)\n", float64(b.avgPersistDurationPerEpochMetrics.Percentile(99))/1e6)
	fmt.Printf("  p75 duration: %.2f(ms)\n", float64(b.avgPersistDurationPerEpochMetrics.Percentile(75))/1e6)

	fmt.Println("// --------- batch persisted db tps -----------")
	fmt.Printf("mean tps: %v\n", b.persistTimer.RateMean())
	fmt.Printf("  m1 tps: %v\n", b.persistTimer.Rate1())
	fmt.Printf("  m5 tps: %v\n", b.persistTimer.Rate5())
	fmt.Printf(" m15 tps: %v\n", b.persistTimer.Rate15())

	fmt.Println("// ---------- epoch fetch duration ------------")
	fmt.Printf("  total epochs: %v\n", b.fetchPerEpochTimer.Count())
	fmt.Printf("  max duration: %.2f(ms)\n", float64(b.fetchPerEpochTimer.Max())/1e6)
	fmt.Printf("  min duration: %.2f(ms)\n", float64(b.fetchPerEpochTimer.Min()/1e6))
	fmt.Printf(" mean duration: %.2f(ms)\n", b.fetchPerEpochTimer.Mean()/1e6)
	fmt.Printf("  p99 duration: %.2f(ms)\n", float64(b.fetchPerEpochTimer.Percentile(99))/1e6)
	fmt.Printf("  p75 duration: %.2f(ms)\n", float64(b.fetchPerEpochTimer.Percentile(75))/1e6)

	fmt.Println("// ------------- epoch fetch tps --------------")
	fmt.Printf("mean tps: %v\n", b.fetchPerEpochTimer.RateMean())
	fmt.Printf("  m1 tps: %v\n", b.fetchPerEpochTimer.Rate1())
	fmt.Printf("  m5 tps: %v\n", b.fetchPerEpochTimer.Rate5())
	fmt.Printf(" m15 tps: %v\n", b.fetchPerEpochTimer.Rate15())
}

func (b *benchmarker) metricPersistDb(start time.Time, state *persistState) {
	persistDuration := b.metricPersistDuration(start)

	b.metricPersistDbRows(int64(state.insertDbRows))
	b.metricPersistEpochs(int64(state.numEpochs()))

	if state.insertDbRows > 0 {
		avgDurationPerDbRow := int64(persistDuration) / int64(state.insertDbRows)
		b.avgPersistDurationPerDbRowMetrics.Update(avgDurationPerDbRow)
	}

	if state.numEpochs() > 0 {
		avgDurationPerEpoch := int64(persistDuration) / int64(state.numEpochs())
		b.avgPersistDurationPerEpochMetrics.Update(avgDurationPerEpoch)
	}
}

func (b *benchmarker) metricPersistEpochs(numEpochs int64) {
	b.persistEpochsMetrics.Update(numEpochs)

	atomic.AddInt64(&b.totalPersistEpochs, numEpochs)
}

func (b *benchmarker) metricPersistDbRows(dbRows int64) {
	b.persistDbRowsMetrics.Update(dbRows)

	atomic.AddInt64(&b.totalPersistDbRows, dbRows)
}

func (b *benchmarker) metricPersistDuration(start time.Time) time.Duration {
	b.persistTimer.UpdateSince(start)

	persistDuration := time.Since(start)
	atomic.AddInt64((*int64)(&b.totalPersistDuration), int64(persistDuration))

	return persistDuration
}

func (b *benchmarker) metricFetchPerEpochDuration(start time.Time) time.Duration {
	b.fetchPerEpochTimer.UpdateSince(start)

	atomic.AddInt64(&b.totalFetchEpochs, 1)

	fetchDuration := time.Since(start)
	atomic.AddInt64((*int64)(&b.totalFetchDuration), int64(fetchDuration))

	return fetchDuration
}
