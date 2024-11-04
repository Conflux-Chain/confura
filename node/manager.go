package node

import (
	"strings"
	"sync"

	"github.com/Conflux-Chain/confura/util/metrics"
	"github.com/buraksezer/consistent"
	"github.com/cespare/xxhash"
)

// nodeFactory factory method to create node instance
type nodeFactory func(group Group, name, url string) (Node, error)

// Manager manages full node cluster, including:
// 1. Monitor node health and disable/enable full node automatically.
// 2. Implements Router interface to route RPC requests to different full nodes
// in manner of consistent hashing.
type Manager struct {
	group    Group
	nodes    map[string]Node        // node name => Node
	hashRing *consistent.Consistent // consistent hashing algorithm
	resolver RepartitionResolver    // support repartition for hash ring
	mu       sync.RWMutex

	// health monitor
	monitorStatuses map[string]monitorStatus // node name => monitor status
	midEpoch        uint64                   // middle epoch of managed full nodes.
}

func NewManager(group Group) *Manager {
	return NewManagerWithRepartition(group, &noopRepartitionResolver{})
}

func NewManagerWithRepartition(group Group, resolver RepartitionResolver) *Manager {
	return &Manager{
		group:           group,
		nodes:           make(map[string]Node),
		resolver:        resolver,
		monitorStatuses: make(map[string]monitorStatus),
		hashRing:        consistent.New(nil, cfg.HashRingRaw()),
	}
}

// Add adds fullnode to monitor
func (m *Manager) Add(nodes ...Node) {
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, n := range nodes {
		if _, ok := m.nodes[n.Name()]; !ok {
			m.nodes[n.Name()] = n
			m.hashRing.Add(n)
		}
	}
}

// Remove removes monitored fullnode
func (m *Manager) Remove(nodeNames ...string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, nn := range nodeNames {
		if node, ok := m.nodes[nn]; ok {
			node.Close()
			delete(m.nodes, nn)
			delete(m.monitorStatuses, nn)
			m.hashRing.Remove(nn)
		}
	}
}

// Get gets monitored fullnode from url
func (m *Manager) Get(nodeName string) (Node, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	n, ok := m.nodes[nodeName]
	return n, ok
}

// List lists all monitored fullnodes
func (m *Manager) List() []Node {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var nodes []Node

	for _, v := range m.nodes {
		nodes = append(nodes, v)
	}

	return nodes
}

// String implements stringer interface
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
		metrics.Registry.Nodes.Routes(m.group.Space(), m.group.String(), "overall").Mark(1)
		// metrics per node route QPS
		metrics.Registry.Nodes.Routes(m.group.Space(), m.group.String(), n.Name()).Mark(1)

		return n.Url()
	}

	return ""
}
