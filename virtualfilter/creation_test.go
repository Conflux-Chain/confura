package virtualfilter

import (
	"context"
	"fmt"
	"testing"

	cfxtypes "github.com/Conflux-Chain/go-conflux-sdk/types"
	ethtypes "github.com/openweb3/web3go/types"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

// Exercise the constructor and accept/establish path, including its cursor
// snapshot. The lifecycle goroutine uses an idle, valid upstream fixture.
func newAcceptedLogSession(t *testing.T, space string, cache int) (*gorm.DB, *testLogSession) {
	t.Helper()
	db, s := newTestLogSession(t, space, cache)
	s.w.session = nilPollingSession
	if space == "eth" {
		f := s.f.(*ethLogFilter)
		s.w.client = &testPollingClient{changes: &ethtypes.FilterChanges{}, chain: func() filterChain { return newEthFilterChain(cache) }}
		created, err := newEthLogFilter(f.logStore, f.worker, f.client, f.crit)
		require.NoError(t, err)
		s.f = created
	} else {
		f := s.f.(*cfxLogFilter)
		s.w.client = &testPollingClient{changes: &cfxtypes.CfxFilterChanges{}, chain: func() filterChain { return newCfxFilterChain(cache) }}
		created, err := newCfxLogFilter(f.logStore, f.worker, f.client, f.crit)
		require.NoError(t, err)
		s.f = created
	}
	return db, s
}

func TestCreatedFilterPreservesInitialRemoved(t *testing.T) {
	for _, emptyRead := range []bool{false, true} {
		for _, replacement := range []bool{false, true} {
			t.Run(fmt.Sprintf("emptyRead=%v/replacement=%v", emptyRead, replacement), func(t *testing.T) {
				_, s := newAcceptedLogSession(t, "eth", 1)
				if emptyRead {
					got, err := s.f.fetch(context.Background())
					require.NoError(t, err)
					require.Zero(t, logCount(got))
				}
				a, b := testEthLog(1), testEthLog(2)
				a.Removed, b.Removed = true, true
				want := []ethtypes.Log{b, a}
				if replacement {
					c := testEthLog(101)
					c.BlockNumber = 1
					want = append(want, c)
				}
				batch := &ethtypes.FilterChanges{Logs: want}
				require.NoError(t, s.persist(batch))
				require.NoError(t, s.w.merge(batch))
				got, err := s.f.fetch(context.Background())
				require.NoError(t, err)
				require.Equal(t, want, got.(*ethtypes.FilterChanges).Logs)
				got, err = s.f.fetch(context.Background())
				require.NoError(t, err)
				require.Zero(t, logCount(got))
			})
		}
	}
}
