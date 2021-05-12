package alert

import (
	"fmt"
	"strings"
	"time"

	"github.com/royeo/dingrobot"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

const (
	dingTalkAlertMsgTpl = "logrus alert notification\ntags:\t%v;\nlevel:\t%v;\nbrief:\t%v;\ndetail:\t%v;\ntime:\t%v\n"
)

var (
	// custom tags are usually used to differentiate between different networks and enviroments
	// such as mainnet/testnet, prod/test/dev or any custom info for more details.
	customTags    []string
	customTagsStr string
)

func init() {
	customTags = viper.GetStringSlice("alert.customTags")
	customTagsStr = strings.Join(customTags, "/")
}

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
	formatter := &logrus.JSONFormatter{}
	detailBytes, _ := formatter.Format(logEntry)

	// Trim last newline char to uniform message format
	detail := strings.TrimSuffix(string(detailBytes), "\n")

	levelStr := logEntry.Level.String()
	nowStr := time.Now().Format("2006-01-02T15:04:05-0700")
	brief := logEntry.Message

	msg := fmt.Sprintf(dingTalkAlertMsgTpl, customTagsStr, levelStr, brief, detail, nowStr)
	atMobiles := viper.GetStringSlice("alert.dingtalk.atMobiles")
	isAtAll := viper.GetBool("alert.dingtalk.isAtAll")

	return hook.dingRobot.SendText(msg, atMobiles, isAtAll)
}
