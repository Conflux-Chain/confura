package test

import (
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/Conflux-Chain/confura/cmd/util"
	validators "github.com/Conflux-Chain/confura/test"
	"github.com/spf13/cobra"
)

func validateEndpoints(websocket bool, endpoints ...string) error {
	for _, endpoint := range endpoints {
		u, err := url.Parse(endpoint)
		if err != nil || u.Hostname() == "" {
			return fmt.Errorf("a valid endpoint URL with a host is required")
		}
		if websocket {
			if u.Scheme != "ws" && u.Scheme != "wss" {
				return fmt.Errorf("subscription endpoints require ws or wss")
			}
		} else if u.Scheme != "http" && u.Scheme != "https" {
			return fmt.Errorf("RPC endpoints require http or https")
		}
	}
	return nil
}

func validateIntervals(scan, sampling time.Duration) error {
	if scan <= 0 || sampling <= 0 {
		return fmt.Errorf("scan and sampling intervals must be positive")
	}
	return nil
}

func validateNetwork(network string) error {
	if !strings.EqualFold(network, "cfx") && !strings.EqualFold(network, "eth") {
		return fmt.Errorf("network must be cfx or eth")
	}
	return nil
}

func runValidator(cmd *cobra.Command, create func() (validators.TestValidator, error)) error {
	life, err := util.NewLifecycle(cmd.Context())
	if err != nil {
		return err
	}
	defer life.Close()
	validator, err := create()
	if err != nil {
		return err
	}
	defer func() { life.Shutdown(); validator.Destroy() }()
	util.RunTracked(life.Ctx, &life.Wg, validator.Run)
	life.Wait()
	return life.Err()
}
