package node

import (
	"strings"

	"github.com/buraksezer/consistent"
	"github.com/cespare/xxhash"
	"github.com/spf13/viper"
)

type Manager struct {
	nodes    map[string]*Node
	hashRing *consistent.Consistent
}

func NewMananger() *Manager {
	manager := Manager{
		nodes: make(map[string]*Node),
	}

	var members []consistent.Member

	for _, url := range viper.GetStringSlice("node.urls") {
		nodeName := manager.url2NodeName(url)
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

func (m *Manager) url2NodeName(url string) string {
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
	nodeName := m.url2NodeName(url)
	if _, ok := m.nodes[nodeName]; !ok {
		node := NewNode(nodeName, url)
		m.nodes[nodeName] = node
		m.hashRing.Add(node)
	}
}

func (m *Manager) Remove(url string) {
	nodeName := m.url2NodeName(url)
	if node, ok := m.nodes[nodeName]; ok {
		node.Close()
		delete(m.nodes, nodeName)
		m.hashRing.Remove(nodeName)
	}
}

func (m *Manager) Get(url string) *Node {
	nodeName := m.url2NodeName(url)
	return m.nodes[nodeName]
}

func (m *Manager) List() []*Node {
	var nodes []*Node

	for _, v := range m.nodes {
		nodes = append(nodes, v)
	}

	return nodes
}

func (m *Manager) String() string {
	var nodes []string

	for n := range m.nodes {
		nodes = append(nodes, n)
	}

	return strings.Join(nodes, ", ")
}

// Distribute distributes a full node by specified key.
func (m *Manager) Distribute(key []byte) *Node {
	// TODO keep consistent even when full node added or removed.
	// Basically, we could cache the key-nodename pair in Redis
	// with proper timeout, e.g. 10 minutes. Then, same key will
	// be distributed with the same full node within 10 minutes,
	// even re-location occurred in hash ring.
	return m.hashRing.LocateKey(key).(*Node)
}
