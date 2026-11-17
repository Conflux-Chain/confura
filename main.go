package main

import (
	"github.com/Conflux-Chain/confura/cmd"
	"github.com/Conflux-Chain/confura/config"
	"github.com/Conflux-Chain/confura/util/pprof"
)

func main() {
	// ensure configuration is initialized first.
	config.Init()
	defer pprof.Close()

	cmd.Execute()
}
