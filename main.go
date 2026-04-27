package main

import (
	"github.com/Conflux-Chain/confura/cmd"
	"github.com/Conflux-Chain/confura/config"
)

func main() {
	// ensure configuration is initialized first.
	config.Init()

	cmd.Execute()
}
