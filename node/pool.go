package node

import (
	"sync"

	"github.com/Conflux-Chain/confura/util/rpc"
	"github.com/pkg/errors"
)

// refNode reusable nodes with reference count
type refNode struct {
	Node
	refCnt int
}

// nodePool manages all full nodes by group
type nodePool struct {
	mu sync.Mutex

	// factory method to create node
	nf nodeFactory
	// node cluster managers by group:
	// group name => node cluster manager
	managers map[Group]*Manager
	// all managed nodes:
	// node name => refNode
	nodes map[string]refNode
}

func newNodePool(nf nodeFactory) *nodePool {
	return &nodePool{
		nf:       nf,
		managers: make(map[Group]*Manager),
		nodes:    make(map[string]refNode),
	}
}

// add adds some node(s) into specific pool group
func (p *nodePool) add(grp Group, urls ...string) error {
	if len(urls) == 0 {
		return nil
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	if _, ok := p.managers[grp]; !ok {
		// create group if not exited yet
		p.managers[grp] = NewManager(grp)
	}

	m := p.managers[grp]
	refNodes := make([]refNode, 0, len(urls))

	for i := range urls {
		nn := rpc.Url2NodeName(urls[i])
		if _, ok := m.Get(nn); ok { // node already grouped?
			continue
		}

		if node, ok := p.nodes[nn]; ok { // node already exists?
			refNodes = append(refNodes, node)
			continue
		}

		n, err := p.nf(grp, nn, urls[i])
		if err != nil {
			return errors.WithMessagef(err, "failed to new node with url %v", urls[i])
		}

		refNodes = append(refNodes, refNode{Node: n})
	}

	for _, rn := range refNodes {
		m.Add(rn)
		rn.Register(m)

		// reference the shared node
		rn.refCnt++
		p.nodes[rn.Name()] = rn
	}

	return nil
}

// del deletes node(s) from specific pool group
func (p *nodePool) del(grp Group, urls ...string) {
	p.mu.Lock()
	defer p.mu.Unlock()

	m, ok := p.managers[grp]
	if !ok { // group not existed
		return
	}

	for i := range urls {
		nn := rpc.Url2NodeName(urls[i])
		if _, ok := m.Get(nn); !ok { // node not grouped?
			continue
		}

		m.Remove(nn)

		if rn, ok := p.nodes[nn]; ok {
			rn.Deregister(m)

			// unreference the shared node
			if rn.refCnt > 1 {
				rn.refCnt--
				p.nodes[nn] = rn
			} else {
				rn.Close()
				delete(p.nodes, nn)
			}
		}
	}

	if len(m.List()) == 0 {
		// uninstall group manager if no node exists anymore
		delete(p.managers, grp)
	}
}

// get gets url of (all or with some excluded) nodes by group
func (p *nodePool) get(grp Group, excluded ...string) (urls []string) {
	p.mu.Lock()
	defer p.mu.Unlock()

	m, ok := p.managers[grp]
	if !ok {
		return nil
	}

	// build exclusive mapset
	excludeset := make(map[string]bool)
	for _, exurl := range excluded {
		excludeset[rpc.Url2NodeName(exurl)] = true
	}

	for _, n := range m.List() {
		if !excludeset[n.Name()] {
			urls = append(urls, n.Url())
		}
	}

	return urls
}

// status returns status for (all or some specific) nodes by group
func (p *nodePool) status(grp Group, included ...string) (res []Status) {
	p.mu.Lock()
	defer p.mu.Unlock()

	mgr, ok := p.managers[grp]
	if !ok { // group not found
		return nil
	}

	includeset := make(map[string]bool)
	for _, url := range included {
		includeset[rpc.Url2NodeName(url)] = true
	}

	// get all group node status
	for _, n := range mgr.List() {
		if len(includeset) == 0 || includeset[n.Name()] {
			res = append(res, n.Status())
		}
	}

	return res
}

// groups lists all available route groups
func (p *nodePool) groups() (res []Group) {
	p.mu.Lock()
	defer p.mu.Unlock()

	for grp := range p.managers {
		res = append(res, grp)
	}

	return res
}

// manager returns the node manager for specific group
func (p *nodePool) manager(group Group) (*Manager, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()

	m, ok := p.managers[group]
	return m, ok
}

func dedupNodeUrls(urls []string) (dedups []string) {
	dupset := make(map[string]bool)

	for _, url := range urls {
		nn := rpc.Url2NodeName(url)
		if !dupset[nn] {
			dedups = append(dedups, url)
		}

		dupset[nn] = true
	}

	return dedups
}
