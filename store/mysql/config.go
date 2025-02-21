package mysql

import (
	"fmt"
	stdLog "log"
	"os"
	"time"

	"github.com/Conflux-Chain/go-conflux-util/dlock"
	"github.com/Conflux-Chain/go-conflux-util/viper"
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
	&epochBlockMap{},
	&bnPartition{},
	&NodeRoute{},
	&dlock.Dlock{},
}

// Config represents the mysql configurations to open a database instance.
type Config struct {
	Enabled bool

	Host     string `default:"127.0.0.1:3306"`
	Username string
	Password string
	Database string

	Dsn string // to support one line config

	ConnMaxLifetime time.Duration `default:"3m"`
	MaxOpenConns    int           `default:"10"`
	MaxIdleConns    int           `default:"10"`
	CreateBatchSize int           `default:"500"`

	AddressIndexedLogEnabled    bool   `default:"true"`
	AddressIndexedLogPartitions uint32 `default:"100"`

	MaxBnRangedArchiveLogPartitions uint32 `default:"5"`
}

func mustNewConfigFromViper(key string) *Config {
	var cfg Config
	viper.MustUnmarshalKey(key, &cfg)

	if !cfg.Enabled {
		return &Config{}
	}

	if len(cfg.Dsn) == 0 {
		return &cfg
	}

	dsnCfg, err := gosql.ParseDSN(cfg.Dsn)
	if err != nil {
		logrus.WithError(err).WithField("dsn", cfg.Dsn).Fatal("Failed to parse db DSN")
	}

	cfg.Host = dsnCfg.Addr
	cfg.Username = dsnCfg.User
	cfg.Password = dsnCfg.Passwd
	cfg.Database = dsnCfg.DBName

	return &cfg
}

// MustNewConfigFromViper creates an instance of Config from Viper or panic on error.
func MustNewConfigFromViper() *Config {
	return mustNewConfigFromViper("store.mysql")
}

func MustNewEthStoreConfigFromViper() *Config {
	return mustNewConfigFromViper("ethstore.mysql")
}

// MustOpenOrCreate creates an instance of store or exits on any erorr.
func (config *Config) MustOpenOrCreate(option StoreOption) *MysqlStore {
	newCreated := config.mustCreateDatabaseIfAbsent()

	db := config.mustNewDB(config.Database)

	// Very naive check on database tables are ready or not.
	// If no database tables are found, we regard the database as a new created one.
	if !newCreated {
		var tables []string
		if err := db.Raw("SHOW TABLES").Scan(&tables).Error; err != nil {
			logrus.WithField("database", config.Database).WithError(err).Fatal("Failed to query database tables")
		}

		newCreated = (len(tables) == 0)
	}

	if newCreated {
		if err := db.Migrator().CreateTable(allModels...); err != nil {
			logrus.WithError(err).Fatal("Failed to create tables")
		}

		ls := NewAddressIndexedLogStore(db, NewContractStore(db), config.AddressIndexedLogPartitions)
		if _, err := ls.CreatePartitionedTables(); err != nil {
			logrus.WithError(err).
				WithField("partitions", config.AddressIndexedLogPartitions).
				Fatal("Failed to create address indexed log tables")
		}
	}

	if sqlDb, err := db.DB(); err != nil {
		logrus.WithError(err).Fatal("Failed to init mysql db")
	} else {
		sqlDb.SetConnMaxLifetime(config.ConnMaxLifetime)
		sqlDb.SetMaxOpenConns(config.MaxOpenConns)
		sqlDb.SetMaxIdleConns(config.MaxIdleConns)
	}

	logrus.Info("MySQL database initialized")

	return mustNewStore(db, config, option)
}

func (config *Config) mustNewDB(database string) *gorm.DB {
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

	// refer to https://github.com/go-sql-driver/mysql#dsn-data-source-name
	dsn := fmt.Sprintf("%v:%v@tcp(%v)/%v?parseTime=true", config.Username, config.Password, config.Host, database)
	if database == config.Database && len(config.Dsn) > 0 {
		dsn = config.Dsn
	}

	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{
		Logger:          gLogger,
		CreateBatchSize: config.CreateBatchSize,
	})

	if err != nil {
		logrus.WithError(err).Fatal("Failed to open mysql")
	}

	return db
}

func (config *Config) mustCreateDatabaseIfAbsent() bool {
	db := config.mustNewDB("")
	if mysqlDb, err := db.DB(); err != nil {
		return false
	} else {
		defer mysqlDb.Close()
	}

	rows, err := db.Raw(fmt.Sprintf("SHOW DATABASES LIKE '%v'", config.Database)).Rows()
	if err != nil {
		logrus.WithError(err).Fatal("Failed to query databases")
	}
	defer rows.Close()

	if rows.Next() {
		return false
	}

	if err = db.Exec("CREATE DATABASE IF NOT EXISTS " + config.Database).Error; err != nil {
		logrus.WithError(err).Fatal("Failed to create database")
	}

	logrus.Info("Create database for the first time")

	return true
}
