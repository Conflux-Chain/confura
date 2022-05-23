package alert

import (
	"fmt"
	"strings"
	"time"

	"github.com/Conflux-Chain/go-conflux-util/viper"
	"github.com/royeo/dingrobot"
)

const (
	dingTalkAlertMsgTpl = "logrus alert notification\ntags:\t%v;\nlevel:\t%v;\nbrief:\t%v;\ndetail:\t%v;\ntime:\t%v\n"
)

type config struct {
	// custom tags are usually used to differentiate between different networks and enviroments
	// such as mainnet/testnet, prod/test/dev or any custom info for more details.
	CustomTags []string `default:"[testnet,dev]"`
	DingTalk   struct {
		Enabled   bool
		WebHook   string
		Secret    string
		AtMobiles []string
		IsAtAll   bool
	}
}

var (
	conf config

	dingTalkCustomTagsStr string
	dingRobot             dingrobot.Roboter
)

func InitDingRobot() {
	viper.MustUnmarshalKey("alert", &conf)

	if conf.DingTalk.Enabled {
		dingTalkCustomTagsStr = strings.Join(conf.CustomTags, "/")
		dingRobot = dingrobot.NewRobot(conf.DingTalk.WebHook)
		dingRobot.SetSecret(conf.DingTalk.Secret)
	}
}

func SendDingTalkTextMessage(level, brief, detail string) error {
	if dingRobot == nil {
		return nil
	}

	nowStr := time.Now().Format("2006-01-02T15:04:05-0700")
	msg := fmt.Sprintf(dingTalkAlertMsgTpl, dingTalkCustomTagsStr, level, brief, detail, nowStr)

	return dingRobot.SendText(msg, conf.DingTalk.AtMobiles, conf.DingTalk.IsAtAll)
}
