package middlewares

import (
	"context"
	"errors"

	"github.com/Conflux-Chain/confura/util/rpc/handlers"
	"github.com/Conflux-Chain/go-conflux-util/viper"
	"github.com/openweb3/go-rpc-provider"
)

var (
	errAccessForbidden = errors.New("access forbidden")
)

type accessControlConfig struct {
	// access control list of RPC methods for VIP only
	VipOnlyAcl []string
}

func MustNewVipOnlyAccessControlMiddlewareFromViper() rpc.HandleCallMsgMiddleware {
	var conf accessControlConfig
	viper.MustUnmarshalKey("accessControl", &conf)

	// restricted RPC methods hashset
	acl := make(map[string]struct{})
	for _, method := range conf.VipOnlyAcl {
		acl[method] = struct{}{}
	}

	return func(next rpc.HandleCallMsgFunc) rpc.HandleCallMsgFunc {
		return func(ctx context.Context, msg *rpc.JsonRpcMessage) *rpc.JsonRpcMessage {
			if _, ok := acl[msg.Method]; !ok { // not in the restriction list?
				return next(ctx, msg)
			}

			if _, ok := handlers.VipStatusFromContext(ctx); !ok {
				// forbidden for non-VIP user
				return msg.ErrorResponse(errAccessForbidden)
			}

			return next(ctx, msg)
		}
	}
}
