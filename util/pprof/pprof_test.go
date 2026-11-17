package pprof

import (
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
)

func TestPprofEndpointSelection(t *testing.T) {
	for _, keys := range [][]string{nil, {"endpoint"}, {"httpEndpoint"}, {"endpoint", "httpEndpoint"}} {
		viper.Reset()
		viper.Set("pprof.enabled", len(keys) > 0)
		for _, key := range keys {
			viper.Set("pprof."+key, "127.0.0.1:0")
		}
		srv, err := StartFromViper()
		require.NoError(t, err)
		if len(keys) == 0 {
			require.Nil(t, srv)
			continue
		}
		client := &http.Client{Timeout: time.Second}
		resp, err := client.Get("http://" + srv.Addr + "/debug/pprof/")
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, resp.StatusCode)
		resp.Body.Close()
		require.NoError(t, srv.Close())
	}
	viper.Reset()
	t.Cleanup(viper.Reset)
	viper.Set("pprof.enabled", true)
	viper.Set("pprof.endpoint", "127.0.0.1:0")
	viper.Set("pprof.httpEndpoint", "127.0.0.1:1")
	_, err := StartFromViper()
	require.ErrorContains(t, err, "conflict")
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer l.Close()
	viper.Set("pprof.endpoint", l.Addr().String())
	viper.Set("pprof.httpEndpoint", l.Addr().String())
	_, err = StartFromViper()
	require.ErrorContains(t, err, "listen pprof")
}
