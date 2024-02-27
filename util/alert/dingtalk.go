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

type AlertConfig struct {
	// custom tags are usually used to differentiate between different networks and enviroments
	// such as mainnet/testnet, prod/test/dev or any custom info for more details.
	CustomTags []string `default:"[testnet,dev]"`
	DingTalk   DingTalkConfig
}

type DingTalkConfig struct {
	Enabled   bool
	WebHook   string
	Secret    string
	AtMobiles []string
	IsAtAll   bool
}

type DingTalkAlerter struct {
	// ding talk robot
	dingrobot.Roboter
	conf DingTalkConfig

	// message template
	customTagStr string
	msgTpl       string
}

func MustNewDingTalkAlerterFromViper() *DingTalkAlerter {
	var conf AlertConfig
	viper.MustUnmarshalKey("alert", &conf)

	if conf.DingTalk.Enabled {
		return NewDingTalkAlerter(conf)
	}

	return nil
}

func NewDingTalkAlerter(conf AlertConfig) *DingTalkAlerter {
	dingRobot := dingrobot.NewRobot(conf.DingTalk.WebHook)
	dingRobot.SetSecret(conf.DingTalk.Secret)

	return &DingTalkAlerter{
		Roboter:      dingRobot,
		conf:         conf.DingTalk,
		msgTpl:       dingTalkAlertMsgTpl,
		customTagStr: strings.Join(conf.CustomTags, "/"),
	}
}

func (dta *DingTalkAlerter) Send(level, brief, detail string) error {
	if dta == nil {
		return nil
	}

	nowStr := time.Now().Format("2006-01-02T15:04:05-0700")
	msg := fmt.Sprintf(dta.msgTpl, dta.customTagStr, level, brief, detail, nowStr)
	return dta.SendText(msg, dta.conf.AtMobiles, dta.conf.IsAtAll)
}
