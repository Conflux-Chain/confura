package handlers

import (
	"net/http"
)

type Middleware func(next http.Handler) http.Handler

type CtxKey string

const (
	CtxKeyRateRegistry = CtxKey("Infura-Rate-Limit-Registry")
	CtxKeyAuthId       = CtxKey("Infura-Auth-ID")

	CtxKeyRealIP      = CtxKey("Infura-Real-IP")
	CtxKeyAccessToken = CtxKey("Infura-Access-Token")
	CtxKeyReqOrigin   = CtxKey("Infura-Req-Origin")
	CtxKeyUserAgent   = CtxKey("Infura-User-Agent")
)
