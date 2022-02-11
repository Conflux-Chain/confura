package main

import (
	// ensure viper based configuration initialized at the very beginning
	_ "github.com/conflux-chain/conflux-infura/config"

	"github.com/conflux-chain/conflux-infura/cmd"
)

func main() {
	cmd.Execute()
}
