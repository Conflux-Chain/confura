package mysql

import (
	"fmt"
	"time"

	"github.com/conflux-chain/conflux-infura/store"
	"github.com/go-sql-driver/mysql"
	"github.com/jinzhu/gorm"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

func init() {
	// Just to load the mysql driver
	mysql.NewConfig()
}

// Config represents the mysql configurations to open a database instance.
type Config struct {
	Username string
	Password string
	Database string

	ConnMaxLifetime time.Duration
	MaxOpenConns    int
	MaxIdleConns    int
}

// NewConfigFromViper creates an instance of Config from Viper.
func NewConfigFromViper() Config {
	return Config{
		Username: viper.GetString("store.mysql.username"),
		Password: viper.GetString("store.mysql.password"),
		Database: viper.GetString("store.mysql.database"),

		ConnMaxLifetime: viper.GetDuration("store.mysql.connMaxLifeTime"),
		MaxOpenConns:    viper.GetInt("store.mysql.maxOpenConns"),
		MaxIdleConns:    viper.GetInt("store.mysql.maxIdleConns"),
	}
}

// MustOpenOrCreate creates an instance of store or exits on any erorr.
func (config *Config) MustOpenOrCreate() store.Store {
	newCreated := config.mustCreateDatabaseIfAbsent()

	db := config.mustNewDB(config.Database)

	if newCreated {
		if err := db.CreateTable(&transaction{}, &block{}, &log{}).Error; err != nil {
			logrus.WithError(err).Fatal("Failed to create tables")
		}
	}

	db.DB().SetConnMaxLifetime(config.ConnMaxLifetime)
	db.DB().SetMaxOpenConns(config.MaxOpenConns)
	db.DB().SetMaxIdleConns(config.MaxIdleConns)

	logrus.Info("MySQL database initialized")

	return mustNewStore(db)
}

func (config *Config) mustNewDB(database string) *gorm.DB {
	dsn := fmt.Sprintf("%v:%v@/%v", config.Username, config.Password, database)
	db, err := gorm.Open("mysql", dsn)

	if logrus.GetLevel() == logrus.DebugLevel {
		db = db.Debug()
	}

	if err != nil {
		logrus.WithError(err).Fatal("Failed to open mysql")
	}

	return db
}

func (config *Config) mustCreateDatabaseIfAbsent() bool {
	db := config.mustNewDB("")
	defer db.Close()

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
