module github.com/Conflux-Chain/confura

go 1.15

require (
	github.com/Conflux-Chain/go-conflux-sdk v1.3.1
	github.com/Conflux-Chain/go-conflux-util v0.0.0-20220216032819-554815f9dbe6
	github.com/buraksezer/consistent v0.9.0
	github.com/cespare/xxhash v1.1.0
	github.com/ethereum/go-ethereum v1.10.15
	github.com/go-redis/redis/v8 v8.8.2
	github.com/go-sql-driver/mysql v1.5.0
	github.com/hashicorp/golang-lru v0.5.5-0.20210104140557-80c98217689d
	github.com/montanaflynn/stats v0.6.6
	github.com/openweb3/go-rpc-provider v0.2.8
	github.com/openweb3/web3go v0.1.1
	github.com/pkg/errors v0.9.1
	github.com/royeo/dingrobot v1.0.1-0.20191230075228-c90a788ca8fd
	github.com/sirupsen/logrus v1.8.1
	github.com/spf13/cobra v1.1.3
	github.com/spf13/viper v1.10.0
	github.com/stretchr/testify v1.7.0
	github.com/zealws/golang-ring v0.0.0-20210116075443-7c86fdb43134
	go.uber.org/multierr v1.6.0
	golang.org/x/time v0.0.0-20220411224347-583f2d630306
	gorm.io/driver/mysql v1.0.5
	gorm.io/gorm v1.22.2
)

// for debugging development
// replace github.com/Conflux-Chain/go-conflux-sdk => ../go-conflux-sdk
