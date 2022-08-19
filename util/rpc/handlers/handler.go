package handlers

import (
	"net/http"
)

type Middleware func(next http.Handler) http.Handler

type CtxKey string

const (
	CtxKeyRealIP       = CtxKey("Infura-Real-IP")
	CtxKeyRateRegistry = CtxKey("Infura-Rate-Limit-Registry")
	CtxAccessToken     = CtxKey("Infura-Access-Token")
)
