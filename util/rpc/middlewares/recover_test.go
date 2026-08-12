package middlewares

import (
	"bytes"
	"context"
	"testing"

	"github.com/Conflux-Chain/confura/util/rpc/handlers"
	"github.com/openweb3/go-rpc-provider"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
)

func TestRecoverCatchesPanicWithoutLoggingAccessToken(t *testing.T) {
	const accessToken = "secretPaidAccessToken1234567890"

	logger := logrus.StandardLogger()
	originalOutput := logger.Out
	var logs bytes.Buffer
	logger.SetOutput(&logs)
	t.Cleanup(func() {
		logger.SetOutput(originalOutput)
	})

	ctx := context.WithValue(context.Background(), handlers.CtxKeyAccessToken, accessToken)
	ctx = context.WithValue(ctx, handlers.CtxKeyRealIP, "192.0.2.1")
	msg := &rpc.JsonRpcMessage{Version: "2.0", ID: []byte("1"), Method: "test_method"}
	next := func(context.Context, *rpc.JsonRpcMessage) *rpc.JsonRpcMessage {
		panic("unexpected middleware panic")
	}

	resp := Recover(next)(ctx, msg)

	require.NotNil(t, resp)
	require.NotNil(t, resp.Error)
	require.Equal(t, errMiddlewareCrashed.Error(), resp.Error.Error())
	require.NotContains(t, logs.String(), accessToken)
	require.Contains(t, logs.String(), accessTokenFingerprint(accessToken))
}
