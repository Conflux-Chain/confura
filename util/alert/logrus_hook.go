package alert

import (
	"strings"

	"github.com/sirupsen/logrus"
)

type LogrusAlertHook struct {
	alerter *DingTalkAlerter
	levels  []logrus.Level
}

func NewLogrusAlertHook(alerter *DingTalkAlerter, lvls []logrus.Level) *LogrusAlertHook {
	return &LogrusAlertHook{alerter: alerter, levels: lvls}
}

func (hook *LogrusAlertHook) Levels() []logrus.Level {
	return hook.levels
}

func (hook *LogrusAlertHook) Fire(logEntry *logrus.Entry) error {
	level := logEntry.Level.String()
	brief := logEntry.Message

	formatter := &logrus.JSONFormatter{}
	detailBytes, _ := formatter.Format(logEntry)
	// Trim last newline char to uniform message format
	detail := strings.TrimSuffix(string(detailBytes), "\n")

	return hook.alerter.Send(level, brief, detail)
}
