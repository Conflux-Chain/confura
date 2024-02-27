package alert

import (
	"testing"

	"github.com/sirupsen/logrus"
)

func TestLogrusAddHooks(t *testing.T) {
	if alerter == nil {
		t.SkipNow()
	}

	// Add alert hook for logrus fatal/warn/error level
	hookLevels := []logrus.Level{logrus.FatalLevel, logrus.WarnLevel, logrus.ErrorLevel}
	logrus.AddHook(NewLogrusAlertHook(alerter, hookLevels))

	// Need to manually check if the message sent to dingtalk group chat
	logrus.Warn("Test logrus add hooks warn")
	logrus.Error("Test logrus add hooks error")
}
