//go:build integration

package mysql

import (
	"context"
	"testing"
	"time"

	"github.com/Conflux-Chain/confura/store"
	cfxtypes "github.com/Conflux-Chain/go-conflux-sdk/types"
	"github.com/stretchr/testify/require"
)

func TestMySQLLogMetadataConnectionCancellation(t *testing.T) {
	ms, db := newIntegrationStore(t, false)
	require.NoError(t, ms.Push(integrationData(1, 2)))
	var c Contract
	require.NoError(t, db.First(&c).Error)
	var topic Topic
	require.NoError(t, db.First(&topic).Error)
	internal := NewInternalContractLogStore(db)
	cases := map[string]func(context.Context) error{
		"getlogs cold address": func(ctx context.Context) error {
			_, err := ms.GetLogs(ctx, store.LogFilter{BlockFrom: 1, BlockTo: 1, Contracts: store.NewVariadicValue("missing")})
			return err
		},
		"scanlogs cold address": func(ctx context.Context) error {
			_, err := ms.ScanLogs(ctx, store.ScanLogParams{Filter: store.ScanLogFilter{BlockFrom: 1, BlockTo: 1, Contract: "missing"}, Limit: 1})
			return err
		},
		"getlogs cold topic": func(ctx context.Context) error {
			_, err := ms.GetLogs(ctx, store.LogFilter{BlockFrom: 1, BlockTo: 1, Topics: []store.VariadicValue[string]{store.NewVariadicValue("missing")}})
			return err
		},
		"scanlogs cold topic": func(ctx context.Context) error {
			_, err := ms.ScanLogs(ctx, store.ScanLogParams{Filter: store.ScanLogFilter{BlockFrom: 1, BlockTo: 1, Topic0: "missing"}, Limit: 1})
			return err
		},
		"contract migration": func(ctx context.Context) error {
			_, err := ms.GetLogs(ctx, store.LogFilter{BlockFrom: 1, BlockTo: 1, Contracts: store.NewVariadicValue(c.Address)})
			return err
		},
		"topic migration": func(ctx context.Context) error {
			_, err := ms.GetLogs(ctx, store.LogFilter{BlockFrom: 1, BlockTo: 1, Topics: []store.VariadicValue[string]{store.NewVariadicValue(topic.Hash)}})
			return err
		},
		"topic normalization": func(ctx context.Context) error {
			_, err := normalizeTopicsToIDs(ms.ts.withContext(ctx), store.NewVariadicValue("missing"))
			return err
		},
		"topic decoding": func(ctx context.Context) error {
			_, err := resolveTopic0Hash(NewTopicStore(db).withContext(ctx), topic.ID)
			return err
		},
		"trace watermark": func(ctx context.Context) error {
			_, err := internal.GetLogs(ctx, CfxInternalContractLogFilter{LogFilter: &cfxtypes.LogFilter{FromEpoch: cfxtypes.NewEpochNumberUint64(1), ToEpoch: cfxtypes.NewEpochNumberUint64(1)}})
			return err
		},
		"trace mapping": func(ctx context.Context) error { _, err := internal.BlockMapping(ctx, 1); return err },
		"trace version": func(ctx context.Context) error { _, err := internal.GetReorgVersionWithContext(ctx); return err },
		"virtual filter": func(ctx context.Context) error {
			_, err := NewVirtualFilterLogStore(db).GetLogs(ctx, "cancel_test", store.LogFilter{BlockFrom: 1, BlockTo: 1})
			return err
		},
	}
	sqlDB, err := db.DB()
	require.NoError(t, err)
	sqlDB.SetMaxOpenConns(1)
	for name, query := range cases {
		t.Run(name, func(t *testing.T) {
			conn, err := sqlDB.Conn(context.Background())
			require.NoError(t, err)
			defer conn.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
			defer cancel()
			result := make(chan error, 1)
			go func() { result <- query(ctx) }()
			select {
			case err := <-result:
				require.ErrorIs(t, err, context.DeadlineExceeded)
			case <-time.After(time.Second):
				t.Fatal("request ignored its deadline while the connection remained occupied")
			}
		})
	}
	// Request-local contexts must not poison the pooled store.
	logs, err := ms.GetLogs(context.Background(), store.LogFilter{BlockFrom: 1, BlockTo: 1, Contracts: store.NewVariadicValue(c.Address)})
	require.NoError(t, err)
	require.Len(t, logs, 1)
}
