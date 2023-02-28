package metrics

import (
	"crypto/md5"
	"sync"

	"github.com/ethereum/go-ethereum/metrics"
)

// taggableRegistry custom registry to collect more taggable metrics
var taggableRegistry = metrics.NewRegistry()

func GetOrRegisterTaggableCounter(name string, tags map[string]string) metrics.Counter {
	tcs := taggableRegistry.GetOrRegister(name, NewTaggableCounterSet).(*TaggableCounterSet)
	return tcs.GetOrRegisterTaggableCounter(tags)
}

// TagSet tag set in the form of key value pairs
type TagSet map[string]string

// Md5 returns MD5 hash for the tag set
func (ts TagSet) Md5() string {
	if len(ts) == 0 {
		return ""
	}

	md5h := md5.New()
	for k, v := range ts {
		md5h.Write([]byte(k + ":" + v))
	}

	return string(md5h.Sum(nil))
}

// TaggableCounter counter with support of custom tags
type TaggableCounter struct {
	metrics.Counter
	tags TagSet
}

func NewTaggableCounter(tags TagSet) *TaggableCounter {
	return &TaggableCounter{
		tags:    tags,
		Counter: metrics.NewCounter(),
	}
}

// TaggableCounterSet containers to hold taggable counters
type TaggableCounterSet struct {
	mu sync.Mutex

	// tagset hash => taggable counter
	counters map[string]*TaggableCounter
}

func NewTaggableCounterSet() *TaggableCounterSet {
	return &TaggableCounterSet{
		counters: make(map[string]*TaggableCounter),
	}
}

// Counters returns a snapshot of all the taggable counters
func (tcs *TaggableCounterSet) Counters() (res []TaggableCounter) {
	tcs.mu.Lock()
	defer tcs.mu.Unlock()

	for _, tc := range tcs.counters {
		res = append(res, *tc)
	}

	return res
}

func (tcs *TaggableCounterSet) GetOrRegisterTaggableCounter(tags TagSet) *TaggableCounter {
	tcs.mu.Lock()
	defer tcs.mu.Unlock()

	tagh := tags.Md5()
	if _, ok := tcs.counters[tagh]; !ok {
		tcs.counters[tagh] = NewTaggableCounter(tags)
	}

	return tcs.counters[tagh]
}

// implement `metrics.Counter` interface

func (tcs *TaggableCounterSet) Dec(i int64) {
	panic("Dec called on a TaggableCounterSet")
}

func (tcs *TaggableCounterSet) Inc(i int64) {
	panic("Inc called on a TaggableCounterSet")
}

func (tcs *TaggableCounterSet) Clear() {
	panic("Clear called on a TaggableCounterSet")
}

func (tcs *TaggableCounterSet) Count() int64 {
	panic("Count called on a TaggableCounterSet")
}

func (tcs *TaggableCounterSet) Snapshot() metrics.Counter {
	panic("Snapshot called on a TaggableCounterSet")
}
