package node

import (
	"strings"
	"sync"

	"github.com/buraksezer/consistent"
	"github.com/cespare/xxhash"
	"github.com/conflux-chain/conflux-infura/util/metrics"
)

// nodeFactory factory methods to create node instance
type nodeFactory func(name, url string, hm HealthMonitor) (Node, error)

// Manager manages full node cluster, including:
// 1. Monitor node health and disable/enable full node automatically.
// 2. Implements Router interface to route RPC requests to different full nodes
// in manner of consistent hashing.
type Manager struct {
	nodes    map[string]Node        // node name => Node
	hashRing *consistent.Consistent // consistent hashing algorithm
	resolver RepartitionResolver    // support repartition for hash ring
	mu       sync.RWMutex

	nodeFactory     nodeFactory       // factory method to create node instance
	nodeName2Epochs map[string]uint64 // node name => epoch
	midEpoch        uint64            // middle epoch of managed full nodes.
}

func NewManager(nf nodeFactory, urls []string) *Manager {
	return NewManagerWithRepartition(nf, urls, &noopRepartitionResolver{})
}

func NewManagerWithRepartition(nf nodeFactory, urls []string, resolver RepartitionResolver) *Manager {
	manager := Manager{
		nodeFactory:     nf,
		nodes:           make(map[string]Node),
		resolver:        resolver,
		nodeName2Epochs: make(map[string]uint64),
	}

	var members []consistent.Member

	for _, url := range urls {
		nodeName := Url2NodeName(url)
		if _, ok := manager.nodes[nodeName]; !ok {
			node, _ := nf(nodeName, url, &manager)
			manager.nodes[nodeName] = node
			members = append(members, node)
		}
	}

	manager.hashRing = consistent.New(members, cfg.HashRingRaw())

	return &manager
}

func (m *Manager) Add(url string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	nodeName := Url2NodeName(url)
	if _, ok := m.nodes[nodeName]; !ok {
		node, _ := m.nodeFactory(nodeName, url, m)
		m.nodes[nodeName] = node
		m.hashRing.Add(node)
	}
}

func (m *Manager) Remove(url string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	nodeName := Url2NodeName(url)
	if node, ok := m.nodes[nodeName]; ok {
		node.Close()
		delete(m.nodes, nodeName)
		delete(m.nodeName2Epochs, nodeName)
		m.hashRing.Remove(nodeName)
	}
}

func (m *Manager) Get(url string) Node {
	m.mu.RLock()
	defer m.mu.RUnlock()

	nodeName := Url2NodeName(url)
	return m.nodes[nodeName]
}

func (m *Manager) List() []Node {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var nodes []Node

	for _, v := range m.nodes {
		nodes = append(nodes, v)
	}

	return nodes
}

func (m *Manager) String() string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var nodes []string

	for n := range m.nodes {
		nodes = append(nodes, n)
	}

	return strings.Join(nodes, ", ")
}

// Distribute distributes a full node by specified key.
func (m *Manager) Distribute(key []byte) Node {
	k := xxhash.Sum64(key)

	m.mu.RLock()
	defer m.mu.RUnlock()

	// Use repartition resolver to distribute if configured.
	if name, ok := m.resolver.Get(k); ok {
		return m.nodes[name]
	}

	member := m.hashRing.LocateKey(key)
	if member == nil { // in case of empty consistent member
		return nil
	}

	node := member.(Node)
	m.resolver.Put(k, node.Name())

	return node
}

// Route implements the Router interface.
func (m *Manager) Route(key []byte) string {
	if n := m.Distribute(key); n != nil {
		// metrics overall route QPS
		metrics.Registry.Nodes.Routes().Mark(1)
		// metrics per node route QPS
		metrics.Registry.Nodes.Routes(n.Name()).Mark(1)

		return n.Url()
	}

	return ""
}
