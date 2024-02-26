//go:build !ci
// +build !ci

package alert

import (
	"os"
	"testing"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
)

func TestDingTalkEnvConfig(t *testing.T) {
	mustInitViperFromConfig()

	dtWebHook := "dingtalk_webhook_url"
	dtSecret := "dingtalk_secret"

	os.Setenv("INFURA_ALERT_DINGTALK_WEBHOOK", dtWebHook)
	os.Setenv("INFURA_ALERT_DINGTALK_SECRET", dtSecret)

	assert.Equal(t, viper.GetString("alert.dingtalk.webhook"), dtWebHook)
	assert.Equal(t, viper.GetString("alert.dingtalk.secret"), dtSecret)
}
