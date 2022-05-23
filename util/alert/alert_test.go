package alert

import (
	"os"
	"testing"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
)

func TestDingTalkEnvConfig(t *testing.T) {
	mustInitViperFromConfig()

	dtWebHook := "dingtalk_webhook_whatever"
	dtSecret := "dingtalk_secret_whatever"

	os.Setenv("CI_ALERT_DINGTALK_WEBHOOK", dtWebHook)
	os.Setenv("CI_ALERT_DINGTALK_SECRET", dtSecret)

	assert.Equal(t, viper.GetString("alert.dingtalk.webhook"), dtWebHook)
	assert.Equal(t, viper.GetString("alert.dingtalk.secret"), dtSecret)
}
