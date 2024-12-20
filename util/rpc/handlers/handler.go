package handlers

import (
	"context"
	"net/http"
)

type Middleware func(next http.Handler) http.Handler

type CtxKey string

const (
	CtxKeyNamespace = CtxKey("Infura-Namespace")

	CtxKeyRateRegistry = CtxKey("Infura-Rate-Limit-Registry")
	CtxKeyAuthId       = CtxKey("Infura-Auth-ID")

	CtxKeyRealIP      = CtxKey("Infura-Real-IP")
	CtxKeyAccessToken = CtxKey("Infura-Access-Token")
	CtxKeyReqOrigin   = CtxKey("Infura-Req-Origin")
	CtxKeyUserAgent   = CtxKey("Infura-User-Agent")
)

func GetNamespaceFromContext(ctx context.Context) (string, bool) {
	namespace, ok := ctx.Value(CtxKeyNamespace).(string)
	return namespace, ok
}
