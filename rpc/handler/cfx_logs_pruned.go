package handler

import (
	"context"
	"fmt"
	"net/http"
	"strings"

	"github.com/Conflux-Chain/confura/node"
	"github.com/Conflux-Chain/confura/rpc/throttle"
	"github.com/Conflux-Chain/confura/store/mysql"
	"github.com/Conflux-Chain/confura/util/rpc"
	sdk "github.com/Conflux-Chain/go-conflux-sdk"
	"github.com/Conflux-Chain/go-conflux-sdk/types"
	"github.com/go-redis/redis/v8"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

const thresholdGetLogs = 1

var (
	errQuotaNotEnough = errors.New("quota not enough")
)

// CfxPrunedLogsHandler RPC handler to get pruned event logs from some archive fullnodes
// in rate limited way or vip mode with dedicated fullnode.
type CfxPrunedLogsHandler struct {
	pool       *node.CfxClientProvider
	store      *mysql.UserStore
	throttling *throttle.RefCounter
}

func NewCfxPrunedLogsHandler(
	pool *node.CfxClientProvider, store *mysql.UserStore, client *redis.Client) *CfxPrunedLogsHandler {
	return &CfxPrunedLogsHandler{
		pool:       pool,
		store:      store,
		throttling: throttle.NewRefCounter(client, thresholdGetLogs),
	}
}

func (h *CfxPrunedLogsHandler) GetLogs(ctx context.Context, filter types.LogFilter) ([]types.Log, error) {
	logs, ok, err := h.getLogsByUser(ctx, filter)
	if err != nil {
		return nil, err
	}

	if ok {
		return logs, nil
	}

	client, err := h.pool.GetClientByIPGroup(ctx, node.GroupCfxArchives)
	if err == node.ErrClientUnavailable {
		return nil, errQuotaNotEnough
	}

	if err != nil {
		return nil, err
	}

	return h.getLogsThrottled(client, filter)
}

func (h *CfxPrunedLogsHandler) getLogsByUser(ctx context.Context, filter types.LogFilter) ([]types.Log, bool, error) {
	request, ok := ctx.Value("request").(*http.Request)
	if !ok {
		// WebSocket have no http.Request object.
		return nil, false, nil
	}

	if request.URL == nil {
		return nil, false, nil
	}

	key := strings.TrimLeft(request.URL.Path, "/")
	if idx := strings.Index(key, "/"); idx > 0 {
		key = key[:idx]
	}

	user, ok, err := h.store.GetUserByKey(key)
	if err != nil {
		logrus.WithError(err).WithField("key", key).Warn("Failed to get user by key")
		return nil, false, err
	}

	if !ok {
		return nil, false, nil
	}

	// TODO cache client for user
	client, err := sdk.NewClient(user.NodeUrl)
	if err != nil {
		logrus.WithError(err).WithFields(logrus.Fields{
			"user": user.Name,
			"node": user.NodeUrl,
		}).Warn("Failed to connect to full node for user")
		return nil, false, err
	}
	defer client.Close()

	logs, err := h.getLogsThrottled(client, filter)
	if err != nil {
		return nil, false, err
	}

	return logs, true, nil
}

func (h *CfxPrunedLogsHandler) getLogsThrottled(cfx sdk.ClientOperator, filter types.LogFilter) ([]types.Log, error) {
	nodeName := rpc.Url2NodeName(cfx.GetNodeURL())
	key := fmt.Sprintf("rpc:throttle:cfx_getLogs:%v", nodeName)
	if !h.throttling.Ref(key) {
		return nil, errQuotaNotEnough
	}
	defer h.throttling.UnrefAsync(key)

	return cfx.GetLogs(filter)
}
