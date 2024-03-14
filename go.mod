module github.com/Conflux-Chain/confura

go 1.15

require (
	github.com/Conflux-Chain/go-conflux-sdk v1.5.9
	github.com/Conflux-Chain/go-conflux-util v0.1.1-0.20231220063312-4844964b9b11
	github.com/Conflux-Chain/web3pay-service v0.0.0-20230609030113-dc3c4d42820a
	github.com/btcsuite/btcutil v1.0.3-0.20201208143702-a53e38424cce
	github.com/buraksezer/consistent v0.9.0
	github.com/cespare/xxhash v1.1.0
	github.com/ethereum/go-ethereum v1.10.15
	github.com/go-redis/redis/v8 v8.8.2
	github.com/go-sql-driver/mysql v1.6.0
	github.com/hashicorp/golang-lru v0.5.5-0.20210104140557-80c98217689d
	github.com/montanaflynn/stats v0.6.6
	github.com/openweb3/go-rpc-provider v0.3.3-0.20240314082803-8ee6dd0c7e4f
	github.com/openweb3/web3go v0.2.6
	github.com/pkg/errors v0.9.1
	github.com/royeo/dingrobot v1.0.1-0.20191230075228-c90a788ca8fd
	github.com/rs/cors v1.7.0
	github.com/sirupsen/logrus v1.9.0
	github.com/spf13/cobra v1.5.0
	github.com/spf13/viper v1.10.0
	github.com/stretchr/testify v1.7.0
	github.com/valyala/fasthttp v1.33.0
	github.com/zealws/golang-ring v0.0.0-20210116075443-7c86fdb43134
	go.uber.org/multierr v1.6.0
	golang.org/x/time v0.0.0-20220411224347-583f2d630306
	gorm.io/driver/mysql v1.3.6
	gorm.io/gorm v1.23.8
)

// for debugging development
// replace github.com/Conflux-Chain/go-conflux-sdk => ../go-conflux-sdk
// replace github.com/Conflux-Chain/web3pay-service => ../web3pay-service
// replace github.com/Conflux-Chain/go-conflux-util => ../go-conflux-util
// replace github.com/openweb3/go-rpc-provider => ../go-rpc-provider
