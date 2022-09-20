package main

import (
	// ensure viper based configuration initialized at the very beginning
	_ "github.com/scroll-tech/rpc-gateway/config"

	"github.com/scroll-tech/rpc-gateway/cmd"
)

func main() {
	cmd.Execute()
}
