package node

import (
	"errors"
	"testing"

	"github.com/Conflux-Chain/confura/store/mysql"
	rpcutil "github.com/Conflux-Chain/confura/util/rpc"
	"github.com/stretchr/testify/require"
)

var errTestPersistence = errors.New("fixture persistence failure")

type testGroupStore struct {
	groups map[string][]string
	fail   bool
}

func (s *testGroupStore) StoreNodeRouteGroup(g *mysql.NodeRouteGroup) error {
	if s.fail {
		return errTestPersistence
	}
	s.groups[g.Name] = append([]string(nil), g.Nodes...)
	return nil
}
func (s *testGroupStore) DelNodeRouteGroup(group string) error {
	if s.fail {
		return errTestPersistence
	}
	delete(s.groups, group)
	return nil
}

func TestPersistentAddRollbackPreservesExistingMembership(t *testing.T) {
	setupNodeTest(t)
	const url = "http://127.0.0.1:19001"
	p := newNodePool(testNodeFactory)
	require.NoError(t, p.add(GroupEthHttp, url))
	require.NoError(t, p.add(GroupEthLogs, url))
	defer p.del(GroupEthLogs, url)
	defer p.del(GroupEthHttp, url)
	db := &testGroupStore{groups: map[string][]string{string(GroupEthHttp): {url}}, fail: true}
	h := &apiHandler{pool: p, dbs: db}
	require.ErrorIs(t, h.addGroupNode(GroupEthHttp, url, true), errTestPersistence)
	require.Equal(t, []string{url}, p.get(GroupEthHttp))
	require.Equal(t, []string{url}, db.groups[string(GroupEthHttp)])
	rn := p.nodes[rpcutil.Url2NodeName(url)]
	require.Equal(t, 2, rn.refCnt)
	require.Zero(t, rn.Node.(*testNode).closes.Load())
	db.fail = false
	require.NoError(t, h.addGroupNode(GroupEthHttp, url, true))
	require.Equal(t, 2, p.nodes[rpcutil.Url2NodeName(url)].refCnt)
	require.Len(t, rn.Node.(*testNode).monitors, 2)
}

func TestPersistentAddRollbackOnlyRemovesNewReferences(t *testing.T) {
	for _, shared := range []bool{false, true} {
		name := "new-node"
		if shared {
			name = "shared-node"
		}
		t.Run(name, func(t *testing.T) {
			setupNodeTest(t)
			const url = "http://127.0.0.1:19001"
			var created *testNode
			p := newNodePool(func(g Group, n, u string) (Node, error) { created = newTestNode(g, n, u); return created, nil })
			if shared {
				require.NoError(t, p.add(GroupEthLogs, url))
				defer p.del(GroupEthLogs, url)
			}
			db := &testGroupStore{groups: make(map[string][]string), fail: true}
			h := &apiHandler{pool: p, dbs: db}
			require.ErrorIs(t, h.addGroupNode(GroupEthHttp, url, true), errTestPersistence)
			require.Empty(t, p.get(GroupEthHttp))
			require.Empty(t, db.groups)
			if shared {
				require.Equal(t, []string{url}, p.get(GroupEthLogs))
				require.Equal(t, 1, p.nodes[rpcutil.Url2NodeName(url)].refCnt)
				require.Zero(t, created.closes.Load())
			} else {
				require.Empty(t, p.nodes)
				require.EqualValues(t, 1, created.closes.Load())
			}
			db.fail = false
			require.NoError(t, h.addGroupNode(GroupEthHttp, url, true))
			defer p.del(GroupEthHttp, url)
			require.NoError(t, h.addGroupNode(GroupEthHttp, url, true))
			require.Equal(t, []string{url}, p.get(GroupEthHttp))
			require.Equal(t, []string{url}, db.groups[string(GroupEthHttp)])
		})
	}
}

func TestPersistenceUnavailableAndFailedRemovePreservePool(t *testing.T) {
	setupNodeTest(t)
	const url = "http://127.0.0.1:19001"
	h := MustNewApiHandler(nil, testNodeFactory, map[Group]UrlConfig{})
	require.ErrorIs(t, h.addGroupNode(GroupEthHttp, url, true), errDbNotAvailableForPersistence)
	require.Empty(t, h.pool.nodes)
	require.NoError(t, h.addGroupNode(GroupEthHttp, url, false))
	defer h.pool.del(GroupEthHttp, url)
	require.ErrorIs(t, h.delGroupNode(GroupEthHttp, url, true), errDbNotAvailableForPersistence)
	h.dbs = &testGroupStore{groups: map[string][]string{string(GroupEthHttp): {url}}, fail: true}
	require.ErrorIs(t, h.delGroupNode(GroupEthHttp, url, true), errTestPersistence)
	require.Equal(t, []string{url}, h.pool.get(GroupEthHttp))
}
