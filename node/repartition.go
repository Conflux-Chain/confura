package node

import (
	"container/list"
	"sync"
	"time"
)

// RepartitionResolver is implemented to support repartition when item added or removed
// in the consistent hash ring.
type RepartitionResolver interface {
	Get(key uint64) (string, bool)
	Put(key uint64, value string)
}

type noopRepartitionResolver struct{}

func (r *noopRepartitionResolver) Get(key uint64) (string, bool) { return "", false }
func (r *noopRepartitionResolver) Put(key uint64, value string)  {}

type partitionInfo struct {
	key      uint64
	node     string
	deadline time.Time
}

type SimpleRepartitionResolver struct {
	key2Items sync.Map
	items     *list.List
	ttl       time.Duration
	mu        sync.Mutex
}

func NewSimpleRepartitionResolver(ttl time.Duration) *SimpleRepartitionResolver {
	return &SimpleRepartitionResolver{
		items: list.New(),
		ttl:   ttl,
	}
}

func (r *SimpleRepartitionResolver) Get(key uint64) (string, bool) {
	value, ok := r.key2Items.Load(key)
	if !ok {
		return "", false
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	item := value.(*list.Element)
	info := item.Value.(partitionInfo)
	now := time.Now()

	// passively check expiration
	if info.deadline.Before(now) {
		r.items.Remove(item)
		r.key2Items.Delete(key)
		return "", false
	}

	// update expiration
	info.deadline = now.Add(r.ttl)
	item.Value = info
	r.items.MoveToBack(item)

	return info.node, true
}

func (r *SimpleRepartitionResolver) Put(key uint64, value string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.gc()

	info := partitionInfo{
		key:      key,
		node:     value,
		deadline: time.Now().Add(r.ttl),
	}

	if value, ok := r.key2Items.Load(key); ok {
		// update item value
		item := value.(*list.Element)
		item.Value = info
		r.items.MoveToBack(item)
	} else {
		// add new item
		item := r.items.PushBack(info)
		r.key2Items.Store(key, item)
	}
}

// gc removes the expired items.
func (r *SimpleRepartitionResolver) gc() {
	now := time.Now()

	for {
		front := r.items.Front()
		if front == nil {
			break
		}

		info := front.Value.(partitionInfo)
		if info.deadline.After(now) {
			break
		}

		r.items.Remove(front)
		r.key2Items.Delete(info.key)
	}
}
