package alert

import (
	"fmt"
	"strings"
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
	msgTpl := "logrus alert notification\nlevel:\t%v;\nbrief:\t%v;\ndetail:\t%v;\ntime:\t%v\n"

	formatter := &logrus.JSONFormatter{}
	detailBytes, _ := formatter.Format(logEntry)

	// Trim last newline char to uniform message format
	detail := strings.TrimSuffix(string(detailBytes), "\n")

	levelStr := logEntry.Level.String()
	nowStr := time.Now().Format("2006-01-02T15:04:05-0700")
	brief := logEntry.Message

	msg := fmt.Sprintf(msgTpl, levelStr, brief, detail, nowStr)
	atMobiles := viper.GetStringSlice("alert.dingtalk.atMobiles")
	isAtAll := viper.GetBool("alert.dingtalk.isAtAll")

	return hook.dingRobot.SendText(msg, atMobiles, isAtAll)
}
