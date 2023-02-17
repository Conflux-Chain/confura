package node

import (
	"sync"
	"time"

	"github.com/Conflux-Chain/confura/store/mysql"
	"github.com/sirupsen/logrus"
)

const (
	nodeRoutePollingInterval = time.Second * 60
)

// nodePool manages all full nodes by group
type nodePool struct {
	mu sync.Mutex

	// factory method to create a node
	nf nodeFactory

	// benchmark groups used for polling against:
	// group id => group info
	id2Groups map[uint32]*mysql.NodeRouteGroup

	// node cluster managers by group:
	// group name => node cluster manager
	managers map[string]*Manager
}

func newNodePool(db *mysql.MysqlStore, nf nodeFactory, groupConf map[Group]UrlConfig) *nodePool {
	// create pre-defined node cluster manager by group
	managers := make(map[string]*Manager)
	for k, v := range groupConf {
		managers[string(k)] = NewManager(k, nf, v.Nodes)
	}

	pool := &nodePool{
		nf:        nf,
		managers:  managers,
		id2Groups: make(map[uint32]*mysql.NodeRouteGroup),
	}

	if db != nil {
		go pool.poll(db)
	}

	return pool
}

// GetManager gets node cluster manager of specific group
func (p *nodePool) GetManager(group Group) (*Manager, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()

	mgr, ok := p.managers[string(group)]
	return mgr, ok
}

// AllManagers makes a snapshot of all managers and then returns
func (p *nodePool) AllManagers() map[Group]*Manager {
	p.mu.Lock()
	defer p.mu.Unlock()

	// make snapshot
	res := make(map[Group]*Manager)
	for grp, mgr := range p.managers {
		res[Group(grp)] = mgr
	}

	return res
}

// poll instantly polls node route groups from database
func (p *nodePool) poll(db *mysql.MysqlStore) {
	ticker := time.NewTicker(nodeRoutePollingInterval)
	defer ticker.Stop()

	// trigger initial poll immediately
	p.pollOnce(db)

	// polls db instantly
	for range ticker.C {
		p.pollOnce(db)
	}
}

func (p *nodePool) pollOnce(db *mysql.MysqlStore) {
	routeGroups, err := db.LoadNodeRouteGroups()
	if err != nil {
		logrus.WithError(err).Error("Failed to poll node route groups")
		return
	}

	if len(routeGroups) == 0 {
		return // no route groups configured
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	// remove node route group
	for gid, group := range p.id2Groups {
		if _, ok := routeGroups[gid]; !ok {
			p.removeGroup(group)
			logrus.WithField("group", group).Info("Node route group removed")
		}
	}

	// add or update node route group
	for gid, group := range routeGroups {
		g, ok := p.id2Groups[gid]
		if !ok { // add
			p.addGroup(group)
			logrus.WithField("group", group).Info("Node route group added")
			continue
		}

		if g.MD5 != group.MD5 { // update
			p.updateGroup(g, group)
			logrus.WithField("group", group).Info("Node route group updated")
		}
	}
}

func (p *nodePool) removeGroup(grp *mysql.NodeRouteGroup) {
	if m, ok := p.managers[grp.Name]; ok {
		m.Close()
	}

	delete(p.managers, grp.Name)
	delete(p.id2Groups, grp.ID)
}

func (p *nodePool) addGroup(grp *mysql.NodeRouteGroup) {
	if m, ok := p.managers[grp.Name]; ok {
		// in case of any pre-defined node cluster manager
		m.Close()
	}

	p.id2Groups[grp.ID] = grp
	p.managers[grp.Name] = NewManager(Group(grp.Name), p.nf, grp.Nodes)
}

func (p *nodePool) updateGroup(old, new *mysql.NodeRouteGroup) {
	p.id2Groups[new.ID] = new
	p.managers[new.Name] = NewManager(Group(new.Name), p.nf, new.Nodes)

	if old.Name != new.Name {
		// group name changed? this shall be rare, but we also need to
		// close the cluster manager associated with the old group
		if mgr, ok := p.managers[old.Name]; ok {
			mgr.Close()
		}

		delete(p.managers, old.Name)
	}
}
