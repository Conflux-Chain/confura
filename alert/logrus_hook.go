package alert

import (
	"fmt"
	"time"

	"github.com/royeo/dingrobot"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

type LogrusAlertHook struct {
	levels    []logrus.Level
	dingRobot dingrobot.Roboter
}

func NewLogrusAlertHook(lvls []logrus.Level) *LogrusAlertHook {
	webHook := viper.GetString("alert.dingtalk.webhook")
	secret := viper.GetString("alert.dingtalk.secret")

	dingRobot := dingrobot.NewRobot(webHook)
	dingRobot.SetSecret(secret)
	return &LogrusAlertHook{levels: lvls, dingRobot: dingRobot}
}

func (hook *LogrusAlertHook) Levels() []logrus.Level {
	return hook.levels
}

func (hook *LogrusAlertHook) Fire(logEntry *logrus.Entry) error {
	levelStr := logEntry.Level.String()
	nowStr := time.Now().Format("2006-01-02T15:04:05-0700")
	msg := fmt.Sprintf("logrus alert notification\nlevel: %v;\nmessage: %v;\ntime: %v\n", levelStr, logEntry.Message, nowStr)

	atMobiles := viper.GetStringSlice("alert.dingtalk.atMobiles")
	isAtAll := viper.GetBool("alert.dingtalk.isAtAll")

	return hook.dingRobot.SendText(msg, atMobiles, isAtAll)
}
