package config

import (
	"net"
	"os"
	"os/exec"
	"testing"

	"github.com/Conflux-Chain/confura/util/pprof"
	"github.com/stretchr/testify/require"
)

func TestInitPprofOnce(t *testing.T) {
	if os.Getenv("CONFURA_PPROF_CHILD") == "1" {
		Init()
		require.NoError(t, pprof.Close())
		return
	}
	for _, keys := range [][]string{{"ENDPOINT"}, {"HTTPENDPOINT"}, {"ENDPOINT", "HTTPENDPOINT"}} {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		addr := l.Addr().String()
		l.Close()
		child := exec.Command(os.Args[0], "-test.run=^TestInitPprofOnce$")
		child.Dir = t.TempDir()
		child.Env = []string{"PATH=" + os.Getenv("PATH"), "HOME=" + child.Dir, "CONFURA_PPROF_CHILD=1", "INFURA_PPROF_ENABLED=true"}
		for _, key := range keys {
			child.Env = append(child.Env, "INFURA_PPROF_"+key+"="+addr)
		}
		out, err := child.CombinedOutput()
		require.NoError(t, err, string(out))
	}
}
