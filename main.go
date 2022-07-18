package main

import (
	// ensure viper based configuration initialized at the very beginning
	_ "github.com/Conflux-Chain/confura/config"

	"github.com/Conflux-Chain/confura/cmd"
)

func main() {
	cmd.Execute()
}
