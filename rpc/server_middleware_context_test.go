package rpc

import (
	"context"
	"testing"
	"time"

	"github.com/Conflux-Chain/confura/node"
	"github.com/Conflux-Chain/confura/store/mysql"
	"github.com/Conflux-Chain/confura/util/rpc/handlers"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

type contextCheckingRouter struct{ entered chan struct{} }

func (r contextCheckingRouter) Route(ctx context.Context, _ node.Group, _ []byte) (string, error) {
	close(r.entered)
	select {
	case <-ctx.Done():
		return "", ctx.Err()
	case <-time.After(time.Second):
		return "", context.DeadlineExceeded
	}
}

func TestProviderMiddlewarePropagatesContext(t *testing.T) {
	for _, space := range []string{"eth", "cfx"} {
		for _, dedicated := range []bool{false, true} {
			name := space + "/public"
			if dedicated {
				name = space + "/dedicated"
			}
			t.Run(name, func(t *testing.T) {
				db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
				require.NoError(t, err)
				sqlDB, err := db.DB()
				require.NoError(t, err)
				sqlDB.SetMaxOpenConns(1)
				defer sqlDB.Close()
				require.NoError(t, db.AutoMigrate(&mysql.NodeRoute{}))
				stores := &mysql.CommonStores{NodeRouteStore: mysql.NewNodeRouteStore(db)}
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				expected := node.Group(space + "http")
				if dedicated {
					expected = node.Group(space + "dedicated")
					require.NoError(t, stores.AddNodeRoute("fixture-tenant", string(expected)))
					ctx = context.WithValue(ctx, handlers.CtxKeyAuthId, "fixture-tenant")
				}
				router := contextCheckingRouter{entered: make(chan struct{})}
				done := make(chan error, 1)
				go func() {
					var group node.Group
					var err error
					if space == "eth" {
						_, group, err = getEthClientFromProviderWithContext(ctx, "eth_blockNumber", node.NewEthClientProvider(nil, stores, router))
					} else {
						_, group, err = getCfxClientFromProviderWithContext(ctx, "cfx_epochNumber", node.NewCfxClientProvider(stores, router))
					}
					if group != expected {
						done <- context.DeadlineExceeded
						return
					}
					done <- err
				}()
				select {
				case <-router.entered:
				case <-time.After(time.Second):
					t.Fatal("middleware did not route")
				}
				cancel()
				select {
				case err := <-done:
					require.ErrorIs(t, err, context.Canceled)
				case <-time.After(300 * time.Millisecond):
					t.Fatal("middleware lost request context")
				}
			})
		}
	}
}
