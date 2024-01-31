package blacklist

import (
	"encoding/json"
	"strings"

	// ensure viper based configuration initialized at the very beginning
	_ "github.com/Conflux-Chain/confura/config"

	"github.com/Conflux-Chain/go-conflux-sdk/types/cfxaddress"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

var (
	// blacklisted contract address set
	blacklistedAddressSet = make(map[string]*BlacklistedAddrInfo)
)

// BlacklistedAddrInfo is used to store blacklisted contract address info.
type BlacklistedAddrInfo struct {
	Address string
	// epoch until which the contract address is ignored, if 0 means forever.
	Epoch uint64
}

func init() {
	// Load blacklisted contract address.
	var blackListAddrStrs string
	if err := viper.UnmarshalKey("sync.blackListAddrs", &blackListAddrStrs); err != nil {
		logrus.WithError(err).Fatal("Failed to load blacklisted contract address")
	}

	if len(blackListAddrStrs) == 0 {
		return
	}

	var addrInfos []*BlacklistedAddrInfo
	if err := json.Unmarshal(([]byte)(blackListAddrStrs), &addrInfos); err != nil {
		logrus.WithError(err).Fatal("Failed to parse blacklisted contract address json")
	}

	for i := range addrInfos {
		blacklistedAddressSet[strings.ToLower(addrInfos[i].Address)] = addrInfos[i]
		logrus.WithField("addrInfo", addrInfos[i]).Info("Loaded blacklisted contract address")
	}
}

// Check if address blacklisted or not for specific epoch height.
func IsAddressBlacklisted(addr *cfxaddress.Address, epochs ...uint64) bool {
	if len(blacklistedAddressSet) == 0 {
		return false
	}

	addrStr := addr.MustGetBase32Address()
	addrStr = strings.ToLower(addrStr)

	info, exists := blacklistedAddressSet[addrStr]
	if !exists {
		return false
	}

	return len(epochs) == 0 || info.Epoch == 0 || epochs[0] <= info.Epoch
}
