package virtualfilter

import (
	"errors"
	"strings"
	"sync"
	"time"

	"github.com/openweb3/go-rpc-provider"
)

type filterType byte

const (
	// filter type - log filter, block filter and pending txn filter
	filterTypeUnknown filterType = iota
	filterTypeLog
	filterTypeBlock
	filterTypePendingTxn
	filterTypeLastIndex
)

var (
	errFilterNotFound = errors.New("filter not found")
)

type filterChanges interface{}

type virtualFilter interface {
	fid() rpc.ID                    // filter ID
	ftype() filterType              // filter type
	nodeName() string               // delegate full node name
	refresh()                       // refresh last polling time
	expired(ttl time.Duration) bool // if this filter is expired with the provided TTL
	fetch() (filterChanges, error)  // fetch filter changes since last polling
	uninstall() (bool, error)       // uninstall filter
}

type filterBase struct {
	id              rpc.ID     // filter ID
	typ             filterType // filter type
	lastPollingTime time.Time  // last polling time
}

func (f *filterBase) fid() rpc.ID {
	return f.id
}

func (f *filterBase) expired(ttl time.Duration) bool {
	return time.Since(f.lastPollingTime) >= ttl
}

func (f *filterBase) refresh() {
	f.lastPollingTime = time.Now()
}

func (f *filterBase) ftype() filterType {
	return f.typ
}

func (f *ethLogFilter) uninstall() (bool, error) {
	return f.worker.reject(f)
}

type filterManager struct {
	mu sync.Mutex

	// virtual filters
	filters map[rpc.ID]virtualFilter
}

func newFilterManager() *filterManager {
	return &filterManager{
		filters: make(map[rpc.ID]virtualFilter),
	}
}

func (m *filterManager) refresh(id rpc.ID) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if v, ok := m.filters[id]; ok {
		v.refresh()
	}
}

func (m *filterManager) get(id rpc.ID) (virtualFilter, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	v, ok := m.filters[id]
	return v, ok
}

func (m *filterManager) add(filter virtualFilter) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.filters[filter.fid()] = filter
}

func (m *filterManager) delete(id rpc.ID) (virtualFilter, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if v, ok := m.filters[id]; ok {
		delete(m.filters, id)
		return v, ok
	}

	return nil, false
}

func (m *filterManager) expire(ttl time.Duration) map[rpc.ID]virtualFilter {
	m.mu.Lock()
	defer m.mu.Unlock()

	res := make(map[rpc.ID]virtualFilter)
	for id, f := range m.filters {
		if !f.expired(ttl) {
			continue
		}

		res[id] = f
		delete(m.filters, id)
	}

	return res
}

// isFilterNotFoundError check if error content contains `filter not found`
func isFilterNotFoundError(err error) bool {
	if err != nil {
		errStr := strings.ToLower(err.Error())
		return strings.Contains(errStr, errFilterNotFound.Error())
	}

	return false
}
