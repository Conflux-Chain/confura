package alert

import (
	"strings"
	"testing"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

func mustInitViperFromConfig() {
	viper.AutomaticEnv()
	viper.SetEnvPrefix("ci")
	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))

	viper.SetConfigName("config")
	viper.SetConfigType("json")
	viper.GetViper().AddConfigPath("../config")

	if err := viper.ReadInConfig(); err != nil {
		panic(errors.WithMessage(err, "Failed to initialize viper"))
	}
}

func TestLogrusAddHooks(t *testing.T) {
	mustInitViperFromConfig()
	// Add alert hook for logrus fatal/warn/error level
	hookLevels := []logrus.Level{logrus.FatalLevel, logrus.WarnLevel, logrus.ErrorLevel}
	logrus.AddHook(NewLogrusAlertHook(hookLevels))

	// Need to manually check if sent the fatal message to dingtalk group chat
	logrus.Warn("Test logrus add hooks warn")
	logrus.Error("Test logrus add hooks error")
	logrus.Fatal("Test logrus add hooks fatal")

}
