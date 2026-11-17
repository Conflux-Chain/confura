package util

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
)

func TestLifecycleSubprocess(t *testing.T) {
	if mode := os.Getenv("CONFURA_LIFECYCLE_CHILD"); mode != "" {
		viper.Set("shutdown.timeout", "1s")
		life, err := NewLifecycle(context.Background())
		require.NoError(t, err)
		defer life.Close()
		life.Wg.Add(1)
		go func() {
			<-life.Ctx.Done()
			fmt.Println("canceled")
			if mode == "normal" {
				life.Wg.Done()
			}
		}()
		fmt.Println("ready")
		life.Wait()
		life.Shutdown()
		require.NoError(t, os.WriteFile(os.Getenv("CONFURA_CLEANUP_MARKER"), []byte("dependencies released"), 0600))
		return
	}
	for _, mode := range []string{"normal", "deadline", "second signal"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			marker := filepath.Join(t.TempDir(), "cleanup")
			child := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestLifecycleSubprocess$")
			child.Env = append(os.Environ(), "CONFURA_LIFECYCLE_CHILD="+mode, "CONFURA_CLEANUP_MARKER="+marker)
			out, err := child.StdoutPipe()
			require.NoError(t, err)
			require.NoError(t, child.Start())
			reader := bufio.NewReader(out)
			line, err := reader.ReadString('\n')
			require.NoError(t, err)
			require.Equal(t, "ready\n", line)
			require.NoError(t, child.Process.Signal(syscall.SIGTERM))
			line, err = reader.ReadString('\n')
			require.NoError(t, err)
			require.Equal(t, "canceled\n", line)
			if mode == "second signal" {
				require.NoError(t, child.Process.Signal(syscall.SIGINT))
			}
			err = child.Wait()
			if mode == "normal" {
				require.NoError(t, err)
				require.FileExists(t, marker)
			} else {
				var exit *exec.ExitError
				require.ErrorAs(t, err, &exit)
				require.Equal(t, 1, exit.ExitCode())
				require.NoFileExists(t, marker)
			}
		})
	}
}
