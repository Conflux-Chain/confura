package middlewares

import (
	"context"
	"errors"
	"time"

	"github.com/Conflux-Chain/confura/util/rate"
	"github.com/Conflux-Chain/confura/util/rpc/handlers"
	"github.com/openweb3/go-rpc-provider"
	"github.com/sirupsen/logrus"
)

const (
	docURL     = "https://doc.confluxnetwork.org/docs/espace/network-endpoints#common-errors"
	errDocHint = "; see " + docURL + " for more details"
)

var (
	errInvalidApiKey = errors.New("invalid api key" + errDocHint)
	errApiKeyExpired = errors.New("api key is already expired" + errDocHint)
)

func Auth() rpc.HandleCallMsgMiddleware {
	// web3pay
	if mw, conf, ok := MustNewWeb3PayMiddlewareFromViper(); ok {
		logrus.WithField("mode", conf.Mode).Info("Web3Pay openweb3 RPC middleware enabled")

		return func(next rpc.HandleCallMsgFunc) rpc.HandleCallMsgFunc {
			return mw(Authenticate(next))
		}
	}

	return Authenticate
}

func Authenticate(next rpc.HandleCallMsgFunc) rpc.HandleCallMsgFunc {
	return func(ctx context.Context, msg *rpc.JsonRpcMessage) *rpc.JsonRpcMessage {
		token, ok := handlers.GetAccessTokenFromContext(ctx)
		if !ok || len(token) == 0 {
			return next(ctx, msg)
		}

		if !handlers.IsAccessTokenValid(token) {
			return msg.ErrorResponse(errInvalidApiKey)
		}

		authId, err := resolveAuthID(ctx)
		if err != nil {
			return msg.ErrorResponse(err)
		}

		if authId != "" {
			ctx = context.WithValue(ctx, handlers.CtxKeyAuthId, authId)
		}

		return next(ctx, msg)
	}
}

func resolveAuthID(ctx context.Context) (string, error) {
	// Web3Pay VIP user
	if vs, ok := handlers.VipStatusFromContext(ctx); ok {
		if vs.Tier == handlers.VipTierNone {
			return "", errInvalidApiKey
		}

		if vs.ExpireAt.Before(time.Now()) {
			return "", errApiKeyExpired
		}

		return vs.ID, nil
	}

	// SVIP user
	if svs, ok := rate.SVipStatusFromContext(ctx); ok {
		return svs.Key, nil
	}

	return "", errInvalidApiKey
}
