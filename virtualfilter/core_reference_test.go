package virtualfilter

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"testing"

	cfxtypes "github.com/Conflux-Chain/go-conflux-sdk/types"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

// The oracle applies wire events to a consumer view. It has no chain pointers,
// cursors or knowledge of the implementation's traversal/compensation logic.
type coreConsumer map[string]cfxtypes.Log

func (v coreConsumer) apply(t *testing.T, changes *cfxtypes.CfxFilterChanges) {
	t.Helper()
	for _, item := range changes.Logs {
		if item.IsRevertLog() {
			for key, log := range v {
				if log.EpochNumber.ToInt().Cmp(item.RevertTo.ToInt()) > 0 {
					delete(v, key)
				}
			}
		} else {
			key := fmt.Sprintf("%s/%s", *item.BlockHash, item.LogIndex.ToInt())
			require.NotContains(t, v, key, "log delivered twice without intervening compensation")
			v[key] = *item.Log
		}
	}
}

func coreBatch(items ...*cfxtypes.SubscriptionLog) *cfxtypes.CfxFilterChanges {
	return &cfxtypes.CfxFilterChanges{Type: "log", Logs: items}
}
func coreBranch(epoch, hash uint64) *cfxtypes.SubscriptionLog {
	log := testCfxLog(epoch)
	h := *testCfxLog(hash).BlockHash
	log.BlockHash = &h
	return log
}

func TestCoreConsumerReference(t *testing.T) {
	changed := testCfxLog(2)
	changed.LogIndex = testCfxLog(3).EpochNumber
	cases := map[string][]*cfxtypes.CfxFilterChanges{
		"changed_payload_in_batch": {coreBatch(coreRevert(1).Logs[0], testCfxLog(2), coreRevert(1).Logs[0], changed, testCfxLog(3))},
		"repeated_hash_in_batch":   {coreBatch(coreRevert(1).Logs[0], testCfxLog(2), testCfxLog(3), coreRevert(1).Logs[0], testCfxLog(2), testCfxLog(3), testCfxLog(4))},
		"deep_then_shallow":        {coreRevert(1), coreBatch(coreBranch(2, 102), coreBranch(3, 103)), coreRevert(2)},
		"remine":                   {coreRevert(1), coreBatch(testCfxLog(2), testCfxLog(3), testCfxLog(4))},
		"remine_then_shallow":      {coreRevert(1), coreBatch(testCfxLog(2), testCfxLog(3), testCfxLog(4)), coreRevert(3)},
		"unread_below_target":      {coreBatch(testCfxLog(5), testCfxLog(6), testCfxLog(7)), coreRevert(6)},
		"empty_then_deeper":        {coreRevert(0), coreRevert(0), coreBatch(coreBranch(1, 101), coreBranch(2, 102))},
		"one_batch":                {coreBatch(coreRevert(1).Logs[0], coreBranch(2, 102), coreBranch(3, 103), coreRevert(2).Logs[0])},
	}
	// Deterministic mixed extension/revert stream with distinct fork hashes.
	rng := rand.New(rand.NewSource(442))
	height := uint64(4)
	var mixed []*cfxtypes.CfxFilterChanges
	for i := 0; i < 80; i++ {
		if height > 0 && rng.Intn(3) == 0 {
			height = uint64(rng.Intn(int(height)))
			mixed = append(mixed, coreRevert(height))
		} else {
			height++
			mixed = append(mixed, coreBatch(coreBranch(height, uint64(1000+i))))
		}
	}
	cases["mixed"] = mixed
	for name, stream := range cases {
		for _, cache := range []int{1, 100} {
			for _, every := range []int{1, 2, 5, 1000} {
				t.Run(fmt.Sprintf("%s/cache=%d/every=%d", name, cache, every), func(t *testing.T) {
					_, s := newAcceptedLogSession(t, "cfx", cache)
					expected, actual := coreConsumer{}, coreConsumer{}
					publish := func(c *cfxtypes.CfxFilterChanges) {
						require.NoError(t, s.persist(c))
						require.NoError(t, s.w.merge(c))
						expected.apply(t, c)
					}
					read := func() {
						got, err := s.f.fetch(context.Background())
						require.NoError(t, err)
						actual.apply(t, got.(*cfxtypes.CfxFilterChanges))
						wantJSON, err := json.Marshal(expected)
						require.NoError(t, err)
						gotJSON, err := json.Marshal(actual)
						require.NoError(t, err)
						require.JSONEq(t, string(wantJSON), string(gotJSON))
						again, err := s.f.fetch(context.Background())
						require.NoError(t, err)
						require.Zero(t, logCount(again), "a consumed response must not repeat")
					}
					publish(coreBatch(testCfxLog(1), testCfxLog(2), testCfxLog(3), testCfxLog(4)))
					read()
					for i, c := range stream {
						publish(c)
						if (i+1)%every == 0 {
							read()
						}
					}
					read()
				})
			}
		}
	}
}

func TestCoreCompensationOwnership(t *testing.T) {
	for _, failure := range []string{"query", "cancel"} {
		t.Run(failure, func(t *testing.T) {
			db, s := newAcceptedLogSession(t, "cfx", 1)
			publish := func(c *cfxtypes.CfxFilterChanges) { require.NoError(t, s.persist(c)); require.NoError(t, s.w.merge(c)) }
			publish(coreBatch(testCfxLog(1), testCfxLog(2), testCfxLog(3), testCfxLog(4)))
			_, err := s.f.fetch(context.Background())
			require.NoError(t, err)
			original := s.f.(*cfxLogFilter)
			fast, err := newCfxLogFilter(original.logStore, original.worker, original.client, original.crit)
			require.NoError(t, err)
			publish(coreRevert(1))
			publish(coreBatch(coreBranch(2, 102), coreBranch(3, 103), coreBranch(4, 104)))
			_, err = fast.fetch(context.Background())
			require.NoError(t, err)
			late, err := newCfxLogFilter(original.logStore, original.worker, original.client, original.crit)
			require.NoError(t, err)
			got, err := late.fetch(context.Background())
			require.NoError(t, err)
			require.Zero(t, logCount(got))
			publish(coreRevert(3))
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			require.NoError(t, db.Callback().Query().After("gorm:query").Register("compensation_fault", func(tx *gorm.DB) {
				if tx.Statement.Table == "vflogs_0xabc_0" {
					if failure == "query" {
						tx.AddError(errors.New("fixture query failure"))
					} else {
						cancel()
					}
				}
			}))
			_, err = s.f.fetch(ctx)
			require.Error(t, err)
			require.NoError(t, db.Callback().Query().Remove("compensation_fault"))
			got, err = s.f.fetch(context.Background())
			require.NoError(t, err)
			changes := got.(*cfxtypes.CfxFilterChanges)
			require.EqualValues(t, 1, changes.Logs[0].RevertTo.ToInt().Uint64())
			require.Len(t, changes.Logs, 3)
			for _, f := range []*cfxLogFilter{fast, late} {
				got, err = f.fetch(context.Background())
				require.NoError(t, err)
				logs := got.(*cfxtypes.CfxFilterChanges).Logs
				require.Len(t, logs, 1)
				require.EqualValues(t, 3, logs[0].RevertTo.ToInt().Uint64())
			}
		})
	}
}
