package virtualfilter

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"testing"

	ethtypes "github.com/openweb3/web3go/types"
	"github.com/stretchr/testify/require"
)

type ethConsumer map[string]ethtypes.Log

func (v ethConsumer) apply(t *testing.T, changes *ethtypes.FilterChanges) {
	t.Helper()
	for _, log := range changes.Logs {
		key := fmt.Sprintf("%s/%d", log.BlockHash, log.Index)
		if log.Removed {
			delete(v, key)
		} else {
			require.NotContains(t, v, key, "duplicate addition without removal")
			v[key] = log
		}
	}
}

func TestEthConsumerReference(t *testing.T) {
	initial := []ethtypes.Log{testEthLog(1), testEthLog(2), testEthLog(3), testEthLog(4)}
	removed := func(logs ...ethtypes.Log) *ethtypes.FilterChanges {
		result := &ethtypes.FilterChanges{}
		for _, log := range logs {
			log.Removed = true
			result.Logs = append(result.Logs, log)
		}
		return result
	}
	streams := map[string][]*ethtypes.FilterChanges{
		"remine":      {removed(initial[3], initial[2], initial[1]), {Logs: initial[1:]}},
		"full_rewind": {removed(initial[3], initial[2], initial[1], initial[0]), {Logs: initial}},
	}
	rng := rand.New(rand.NewSource(442))
	canonical := append([]ethtypes.Log(nil), initial...)
	var mixed []*ethtypes.FilterChanges
	for i := 0; i < 80; i++ {
		if len(canonical) > 0 && rng.Intn(3) == 0 {
			to := rng.Intn(len(canonical))
			batch := &ethtypes.FilterChanges{}
			for j := len(canonical) - 1; j >= to; j-- {
				log := canonical[j]
				log.Removed = true
				batch.Logs = append(batch.Logs, log)
			}
			canonical = canonical[:to]
			mixed = append(mixed, batch)
		} else {
			log := testEthLog(uint64(1000 + i))
			log.BlockNumber = uint64(len(canonical) + 1)
			canonical = append(canonical, log)
			mixed = append(mixed, &ethtypes.FilterChanges{Logs: []ethtypes.Log{log}})
		}
	}
	streams["mixed"] = mixed
	for name, stream := range streams {
		for _, cache := range []int{1, 100} {
			for _, every := range []int{1, 2, 5, 1000} {
				t.Run(fmt.Sprintf("%s/cache=%d/every=%d", name, cache, every), func(t *testing.T) {
					_, s := newAcceptedLogSession(t, "eth", cache)
					expected, actual := ethConsumer{}, ethConsumer{}
					publish := func(c *ethtypes.FilterChanges) {
						require.NoError(t, s.persist(c))
						require.NoError(t, s.w.merge(c))
						expected.apply(t, c)
					}
					read := func() {
						got, err := s.f.fetch(context.Background())
						require.NoError(t, err)
						actual.apply(t, got.(*ethtypes.FilterChanges))
						wantJSON, err := json.Marshal(expected)
						require.NoError(t, err)
						gotJSON, err := json.Marshal(actual)
						require.NoError(t, err)
						require.JSONEq(t, string(wantJSON), string(gotJSON))
						got, err = s.f.fetch(context.Background())
						require.NoError(t, err)
						require.Zero(t, logCount(got))
					}
					publish(&ethtypes.FilterChanges{Logs: initial})
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
