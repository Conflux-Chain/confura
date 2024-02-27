package alert

import (
	"os"
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
)

var (
	alerter *DingTalkAlerter
)

// Please set the following enviroments to start:
// `TEST_DINGTALK_WEBHOOK`: webhook URL for dingtalk channel.
// `TEST_DINGTALK_SECRET`: secret for authentication.

func TestMain(m *testing.M) {
	if err := setup(); err != nil {
		panic(errors.WithMessage(err, "failed to setup"))
	}

	code := m.Run()

	if err := teardown(); err != nil {
		panic(errors.WithMessage(err, "failed to tear down"))
	}

	os.Exit(code)
}

func setup() error {
	webhook := os.Getenv("TEST_DINGTALK_WEBHOOK")
	secret := os.Getenv("TEST_DINGTALK_SECRET")

	if len(webhook) == 0 || len(secret) == 0 {
		return nil
	}

	alerter = NewDingTalkAlerter(AlertConfig{
		DingTalk: DingTalkConfig{
			Enabled: true,
			WebHook: webhook,
			Secret:  secret,
		},
	})

	return nil
}

func teardown() (err error) {
	// nothing needs to be done
	return nil
}

func TestDingTalkAlert(t *testing.T) {
	if alerter == nil {
		t.SkipNow()
	}

	err := alerter.Send("info", "hi", "this is a test")
	assert.NoError(t, err)
}
