package mysql

import (
	"fmt"
	stdLog "log"
	"os"
	"strings"
	"time"

	"github.com/Conflux-Chain/confura/store"
	"github.com/Conflux-Chain/confura/util/viper"
	"github.com/Conflux-Chain/go-conflux-util/dlock"
	gosql "github.com/go-sql-driver/mysql"
	"github.com/sirupsen/logrus"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	gormLogger "gorm.io/gorm/logger"
)

// auto migrating table models
var allModels = []interface{}{
	&transaction{},
	&block{},
	&conf{},
	&RateLimit{},
	&User{},
	&Contract{},
	&Topic{},
	&epochBlockMap{},
	&bnPartition{},
	&NodeRoute{},
	&dlock.Dlock{},
	&InternalContractLog{},
	&CfxTraceSyncEpochBlockMap{},
}

// GetLogsRangeScanConfig controls the universal-log fast path in either space.
type GetLogsRangeScanConfig struct {
	Enabled      bool
	MinAddresses int    `default:"32"`
	MaxBlocks    uint64 `default:"1"` // zero means unlimited
}

// Config represents the mysql configurations to open a database instance.
type Config struct {
	Enabled                         bool
	GetLogsMultiAddressOptimization GetLogsRangeScanConfig

	Host     string `default:"127.0.0.1:3306"`
	Username string
	Password string
	Database string

	Dsn string // to support one line config

	ConnMaxLifetime time.Duration `default:"3m"`
	MaxOpenConns    int           `default:"10"`
	MaxIdleConns    int           `default:"10"`
	CreateBatchSize int           `default:"500"`

	AddressIndexedLogPartitions uint32 `default:"100"`
	TopicIndexedLogPartitions   uint32 `default:"10"`

	MaxBnRangedArchiveLogPartitions uint32 `default:"5"`
}

func NewConfigFromViper(network string) (*Config, error) {
	key := "store.mysql"
	switch strings.ToLower(network) {
	case "cfx":
	case "eth":
		key = "ethstore.mysql"
	default:
		return nil, fmt.Errorf("invalid network space %q", network)
	}
	return configFromViperKey(key)
}

func configFromViperKey(key string) (*Config, error) {
	var cfg Config
	if err := viper.UnmarshalKey(key, &cfg); err != nil {
		return nil, fmt.Errorf("decode %s: %w", key, err)
	}
	if cfg.GetLogsMultiAddressOptimization.Enabled && cfg.GetLogsMultiAddressOptimization.MinAddresses <= 0 {
		return nil, fmt.Errorf("getlogsMultiAddressOptimization.minAddresses must be positive")
	}
	if cfg.Enabled && cfg.Dsn != "" {
		dsn, err := gosql.ParseDSN(cfg.Dsn)
		if err != nil {
			return nil, fmt.Errorf("invalid database DSN")
		}
		cfg.Host, cfg.Username, cfg.Password, cfg.Database = dsn.Addr, dsn.User, dsn.Passwd, dsn.DBName
	}
	return &cfg, nil
}

func mustNewConfigFromViper(key string) *Config {
	cfg, err := configFromViperKey(key)
	if err != nil {
		logrus.WithError(err).Fatal("Invalid database configuration")
	}
	return cfg
}

// MustNewConfigFromViper creates an instance of Config from Viper or panic on error.
func MustNewConfigFromViper() *Config {
	return mustNewConfigFromViper("store.mysql")
}

func MustNewEthStoreConfigFromViper() *Config {
	return mustNewConfigFromViper("ethstore.mysql")
}

// MustOpenOrCreate creates an instance of gorm database or exits on any error.
func (config *Config) MustOpenOrCreate() *gorm.DB {
	db, err := config.OpenOrCreate()
	if err != nil {
		logrus.WithError(err).Fatal("Failed to initialize MySQL database")
	}
	return db
}

// Open opens an existing database. Management commands must not create unrelated
// databases or initialize chain-data tables.
func (config *Config) Open() (*gorm.DB, error) {
	db, err := config.newDB(config.Database)
	if err != nil {
		return nil, err
	}
	sqlDB, err := db.DB()
	if err != nil {
		return nil, err
	}
	sqlDB.SetConnMaxLifetime(config.ConnMaxLifetime)
	sqlDB.SetMaxOpenConns(config.MaxOpenConns)
	sqlDB.SetMaxIdleConns(config.MaxIdleConns)
	return db, nil
}

func (config *Config) OpenOrCreate() (db *gorm.DB, err error) {
	created, err := config.createDatabaseIfAbsent()
	if err != nil {
		return nil, err
	}
	db, err = config.Open()
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			if sqlDB, e := db.DB(); e == nil {
				sqlDB.Close()
			}
		}
	}()
	if !created {
		var tables []string
		if err = db.Raw("SHOW TABLES").Scan(&tables).Error; err != nil {
			return db, err
		}
		created = len(tables) == 0
	}
	if created {
		if err = db.Migrator().CreateTable(allModels...); err != nil {
			return db, err
		}
		ls := NewAddressIndexedLogStore[store.ChainData](db, NewContractStore(db), NewTopicStore(db), config.AddressIndexedLogPartitions)
		if _, err = ls.CreatePartitionedTables(); err != nil {
			return db, err
		}
		tls := NewTopicIndexedLogStore[store.ChainData](db, NewTopicStore(db), config.TopicIndexedLogPartitions)
		if _, err = tls.CreatePartitionedTables(); err != nil {
			return db, err
		}
	}
	logrus.Info("MySQL database initialized")
	return db, nil
}

func (config *Config) newDB(database string) (*gorm.DB, error) {
	logrusLogLevel := logrus.GetLevel()
	gLogLevel := gormLogger.Warn

	// map log level of logrus to that of gorm
	switch {
	case logrusLogLevel <= logrus.ErrorLevel:
		gLogLevel = gormLogger.Error
	case logrusLogLevel >= logrus.DebugLevel:
		// gorm info log level is kind of too verbose
		gLogLevel = gormLogger.Info
	}

	// create gorm logger by customizing the default logger
	gLogger := gormLogger.New(
		stdLog.New(os.Stdout, "\r\n", stdLog.LstdFlags), // io writer
		gormLogger.Config{
			SlowThreshold:             time.Millisecond * 200, // slow SQL threshold (200ms)
			LogLevel:                  gLogLevel,              // log level
			IgnoreRecordNotFoundError: true,                   // never logging on ErrRecordNotFound error
			Colorful:                  true,                   // use colorful print
		},
	)

	var dsn *gosql.Config
	if config.Dsn != "" {
		var err error
		dsn, err = gosql.ParseDSN(config.Dsn)
		if err != nil {
			return nil, fmt.Errorf("invalid database DSN")
		}
	} else {
		dsn = gosql.NewConfig()
		dsn.Net, dsn.Addr, dsn.User, dsn.Passwd, dsn.ParseTime = "tcp", config.Host, config.Username, config.Password, true
	}
	dsn.DBName = database

	db, err := gorm.Open(mysql.Open(dsn.FormatDSN()), &gorm.Config{
		Logger:          gLogger,
		CreateBatchSize: config.CreateBatchSize,
	})

	if err != nil {
		return nil, fmt.Errorf("open MySQL database: %w", err)
	}

	return db, nil
}

func (config *Config) createDatabaseIfAbsent() (bool, error) {
	if config.Database == "" {
		return false, fmt.Errorf("database name must not be empty")
	}
	db, err := config.newDB("")
	if err != nil {
		return false, err
	}
	sqlDB, err := db.DB()
	if err != nil {
		return false, err
	}
	defer sqlDB.Close()
	var count int64
	if err := db.Raw("SELECT COUNT(*) FROM information_schema.SCHEMATA WHERE SCHEMA_NAME = ?", config.Database).Scan(&count).Error; err != nil {
		return false, err
	}
	if count != 0 {
		return false, nil
	}
	name := "`" + strings.ReplaceAll(config.Database, "`", "``") + "`"
	if err := db.Exec("CREATE DATABASE IF NOT EXISTS " + name).Error; err != nil {
		return false, err
	}
	return true, nil
}
