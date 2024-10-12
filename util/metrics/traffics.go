package metrics

import (
	"container/heap"
	"sync"
	"time"

	metricUtil "github.com/Conflux-Chain/go-conflux-util/metrics"
	"github.com/ethereum/go-ethereum/metrics"
)

var (
	tcOncer   sync.Once
	defaultTc *timeWindowTrafficCollector
)

func DefaultTrafficCollector() TrafficCollector {
	if !metrics.Enabled {
		return &noopTrafficCollector{}
	}

	tcOncer.Do(func() {
		defaultTc = newTimeWindowTrafficCollector(time.Minute, 5)
	})

	return defaultTc
}

// TrafficCollector collects traffic hits and calculate topK stats.
type TrafficCollector interface {
	MarkHit(source string)
	TopkVisitors(k int) []Visitor
}

type noopTrafficCollector struct{}

func (ntc *noopTrafficCollector) MarkHit(source string)        {}
func (ntc *noopTrafficCollector) TopkVisitors(k int) []Visitor { return nil }

// Visitor visitor traffic such as vistor source and hit count
type Visitor struct {
	Source string // visitor source
	Hits   int    // visitor hit count
}

// twTrafficSlotData time window traffic slot data
type twTrafficSlotData map[string]int // source => hits

type twTrafficSlotAggregator struct{}

// implements `SlotAggregatorCloneable` interface
func (twTrafficSlotAggregator) Add(acc, val twTrafficSlotData) twTrafficSlotData {
	if acc == nil {
		acc = make(twTrafficSlotData, len(val))
	}

	for k, v := range val {
		acc[k] += v
	}

	return acc
}

func (twTrafficSlotAggregator) Sub(acc, val twTrafficSlotData) twTrafficSlotData {
	if acc == nil { // This should never happen, but just in case
		return make(twTrafficSlotData)
	}

	for k, v := range val {
		acc[k] -= v
		if acc[k] <= 0 {
			delete(acc, k)
		}
	}

	return acc
}

func (twTrafficSlotAggregator) Clone(v twTrafficSlotData) twTrafficSlotData {
	if v == nil {
		return nil
	}

	copy := make(twTrafficSlotData, len(v))
	for k, v := range v {
		copy[k] = v
	}

	return copy
}

// timeWindowTrafficCollector collects traffic hits using sliding window
type timeWindowTrafficCollector struct {
	// For our usage context now, memory shouldn't be a problem since the
	// number of unique visitors (identified by source) is quite limited
	// (far less than a million).
	window *metricUtil.TimeWindow[twTrafficSlotData] // traffic data within a sliding time window.
}

func newTimeWindowTrafficCollector(
	slotInterval time.Duration, numSlots int) *timeWindowTrafficCollector {

	return &timeWindowTrafficCollector{
		window: metricUtil.NewTimeWindowCloneable(slotInterval, numSlots, twTrafficSlotAggregator{}),
	}
}

// MarkHit mark hits from a visitor source
func (tc *timeWindowTrafficCollector) MarkHit(source string) {
	tc.window.Add(twTrafficSlotData{source: 1})
}

// TopkVisitors statisticize topK visitors.
// We snapshot the current visitor traffic data instantly on which we
// also build a TopK min-heap to return the visitors with the topK most
// traffic hits.
//
// The time complexity for this operation is O(nlog(n)), but for our
// usage context, the number of unique visitors is quite limited
// (far less than a million), besides all operation is totally memory
// based, so there should be no performance bottleneck for usage.
func (tc *timeWindowTrafficCollector) TopkVisitors(k int) []Visitor {
	if k <= 0 {
		return nil
	}

	tdata := tc.window.Data()
	if tdata == nil { // no data
		return nil
	}

	topkHeap := topkVisitorHeap(make([]*visitorItem, 0, k+1))
	for src, hits := range tdata {
		vi := &visitorItem{
			Visitor: Visitor{Source: src, Hits: hits},
		}

		if topkHeap.Len() < k { // not enough items
			heap.Push(&topkHeap, vi)
			continue
		}

		if topkHeap[0].Hits >= hits { // heap top is bigger
			continue
		}

		// otherwise pop heap top and push the new one
		heap.Pop(&topkHeap)
		heap.Push(&topkHeap, vi)
	}

	// pop the heap for TopK visitor statistics
	res := make([]Visitor, topkHeap.Len())
	for topkHeap.Len() > 0 {
		item := heap.Pop(&topkHeap).(*visitorItem)
		res[topkHeap.Len()] = item.Visitor
	}

	return res
}

// visitorItem used for TopK visitors heap item
type visitorItem struct {
	Visitor // of which hit count is used as priority

	// item index in the heap array, needed by update and maintained
	// by `heap.Interface` method.
	index int
}

// topkVisitorHeap min heap which implements `heap.Interface` and holds
// visitors for TopK stats
type topkVisitorHeap []*visitorItem

// implements `heap.Interface`

func (h topkVisitorHeap) Len() int { return len(h) }

func (h topkVisitorHeap) Less(i, j int) bool {
	return h[i].Hits < h[j].Hits
}

func (h topkVisitorHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].index = i
	h[j].index = j
}

func (h *topkVisitorHeap) Push(x interface{}) {
	n := len(*h)
	item := x.(*visitorItem)
	item.index = n
	*h = append(*h, item)
}

func (h *topkVisitorHeap) Pop() interface{} {
	old := *h
	n := len(old)
	item := old[n-1]
	old[n-1] = nil  // avoid memory leak
	item.index = -1 // for safety
	*h = old[0 : n-1]
	return item
}
