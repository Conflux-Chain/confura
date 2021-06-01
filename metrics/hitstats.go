package metrics

import (
	"sync/atomic"

	"github.com/conflux-chain/conflux-infura/util"
	"github.com/ethereum/go-ethereum/metrics"
)

type HitStats uint64

func NewHitStats(hits, visits uint32) *HitStats {
	v := HitStats(uint64(hits)<<32 | uint64(visits))
	return (*HitStats)(&v)
}

func (hs *HitStats) Peek() (hits uint32, visits uint32) {
	hits = uint32(*hs >> 32)
	visits = uint32(*hs & 0xFFFFFFFF)

	return hits, visits
}

func (hs *HitStats) Incr(hit bool) *HitStats {
	incv := uint64(1)
	if hit {
		incv = 0x1<<32 | 0x1
	}

	atomic.AddUint64((*uint64)(hs), incv)
	return hs
}

func (hs *HitStats) GetRatio() (int64, bool) {
	hits, visits := hs.Peek()

	if visits == 0 {
		return 0, false
	}

	return int64(hits) * 10000 / int64(visits), true // two digits float precision
}

type HitStatsCollector struct {
	metricHitStatsMap      util.ConcurrentMap // metric key -> *HitStats
	metricHitRatioGaugeMap util.ConcurrentMap // metric key -> metrics.Gauge
}

func NewHitStatsCollector() *HitStatsCollector {
	return &HitStatsCollector{}
}

func (hsc *HitStatsCollector) CollectHitStats(metric string, hit bool) {
	v, _ := hsc.metricHitStatsMap.LoadOrStoreFn(metric, func(k interface{}) interface{} {
		return NewHitStats(0, 0)
	})

	hs := v.(*HitStats)

	hrv, ok := hs.Incr(hit).GetRatio()
	if !ok {
		return
	}

	v, _ = hsc.metricHitRatioGaugeMap.LoadOrStoreFn(metric, func(k interface{}) interface{} {
		return metrics.GetOrRegisterGauge(metric, nil)
	})

	hrg := v.(metrics.Gauge)
	hrg.Update(hrv)
}
