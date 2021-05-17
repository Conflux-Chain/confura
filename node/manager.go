package node

import (
	"strings"
	"sync"

	"github.com/buraksezer/consistent"
	"github.com/cespare/xxhash"
	"github.com/spf13/viper"
)

type Manager struct {
	nodes    map[string]*Node
	hashRing *consistent.Consistent
	resolver RepartitionResolver
	mu       sync.RWMutex
}

func NewMananger(resolver ...RepartitionResolver) *Manager {
	manager := Manager{
		nodes: make(map[string]*Node),
	}

	if len(resolver) == 0 {
		manager.resolver = &noopRepartitionResolver{}
	} else {
		manager.resolver = resolver[0]
	}

	var members []consistent.Member

	for _, url := range viper.GetStringSlice("node.urls") {
		nodeName := url2NodeName(url)
		if _, ok := manager.nodes[nodeName]; !ok {
			node := NewNode(nodeName, url)
			manager.nodes[nodeName] = node
			members = append(members, node)
		}
	}

	manager.hashRing = consistent.New(members, consistent.Config{
		PartitionCount:    viper.GetInt("node.hashring.partitionCount"),
		ReplicationFactor: viper.GetInt("node.hashring.replicationFactor"),
		Load:              viper.GetFloat64("node.hashring.load"),
		Hasher:            &manager,
	})

	return &manager
}

func url2NodeName(url string) string {
	nodeName := strings.ToLower(url)
	nodeName = strings.TrimPrefix(nodeName, "http://")
	nodeName = strings.TrimPrefix(nodeName, "https://")
	nodeName = strings.TrimPrefix(nodeName, "/")
	if idx := strings.Index(nodeName, ":"); idx != -1 {
		nodeName = nodeName[:idx]
	}
	return nodeName
}

// Sum64 implements the consistent.Hasher interface.
func (m *Manager) Sum64(data []byte) uint64 {
	return xxhash.Sum64(data)
}

func (m *Manager) Add(url string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	nodeName := url2NodeName(url)
	if _, ok := m.nodes[nodeName]; !ok {
		node := NewNode(nodeName, url)
		m.nodes[nodeName] = node
		m.hashRing.Add(node)
	}
}

func (m *Manager) Remove(url string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	nodeName := url2NodeName(url)
	if node, ok := m.nodes[nodeName]; ok {
		node.Close()
		delete(m.nodes, nodeName)
		m.hashRing.Remove(nodeName)
	}
}

func (m *Manager) Get(url string) *Node {
	m.mu.RLock()
	defer m.mu.RUnlock()

	nodeName := url2NodeName(url)
	return m.nodes[nodeName]
}

func (m *Manager) List() []*Node {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var nodes []*Node

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
func (m *Manager) Distribute(key []byte) *Node {
	k := m.Sum64(key)

	m.mu.RLock()
	defer m.mu.RUnlock()

	if name, ok := m.resolver.Get(k); ok {
		return m.nodes[name]
	}

	node := m.hashRing.LocateKey(key).(*Node)
	m.resolver.Put(k, node.Name())

	return node
}

// Route implements the Router interface.
func (m *Manager) Route(key []byte) string {
	return m.Distribute(key).GetNodeURL()
}
