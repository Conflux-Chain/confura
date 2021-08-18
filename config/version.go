package config

import (
	"runtime"

	"github.com/sirupsen/logrus"
)

var (
	Version   string
	GitCommit string

	BuildDate string
	BuildOS   string
	BuildArch string
)

func init() {
	BuildOS = runtime.GOOS
	BuildArch = runtime.GOARCH
}

func DumpVersionInfo() {
	strFormat := "%-12v%v\n"

	logrus.Infof(strFormat, "Version:", Version)
	logrus.Infof(strFormat, "Git Commit:", GitCommit)
	logrus.Infof(strFormat, "Build OS:", BuildOS)
	logrus.Infof(strFormat, "Build Arch:", BuildArch)
	logrus.Infof(strFormat, "Build Date:", BuildDate)
}
