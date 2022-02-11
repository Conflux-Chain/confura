module github.com/conflux-chain/conflux-infura

go 1.15

require (
	github.com/Conflux-Chain/go-conflux-sdk v1.0.27
	github.com/Conflux-Chain/go-conflux-util v0.0.0-20220211025053-ea75aa89a966
	github.com/buraksezer/consistent v0.9.0
	github.com/cespare/xxhash v1.1.0
	github.com/ethereum/go-ethereum v1.10.15
	github.com/go-redis/redis/v8 v8.8.2
	github.com/go-sql-driver/mysql v1.5.0
	github.com/montanaflynn/stats v0.6.6
	github.com/nsf/jsondiff v0.0.0-20210303162244-6ea32392771e
	github.com/openweb3/web3go v0.0.0-20220124104644-6ec2b09ded80
	github.com/pkg/errors v0.9.1
	github.com/royeo/dingrobot v1.0.1-0.20191230075228-c90a788ca8fd
	github.com/selvatico/go-mocket v1.0.7
	github.com/sirupsen/logrus v1.8.1
	github.com/spf13/cobra v1.1.3
	github.com/spf13/viper v1.10.0
	github.com/stretchr/testify v1.7.0
	github.com/zealws/golang-ring v0.0.0-20210116075443-7c86fdb43134
	go.uber.org/multierr v1.6.0
	gorm.io/driver/mysql v1.0.5
	gorm.io/gorm v1.22.2
	gorm.io/hints v1.1.0
)

// replace github.com/Conflux-Chain/go-conflux-sdk => ../go-conflux-sdk
