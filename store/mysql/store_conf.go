package mysql

import (
	"crypto/md5"
	"encoding/json"
	"strconv"
	"time"

	"github.com/Conflux-Chain/confura/util/rate"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const (
	MysqlConfKeyReorgVersion = "reorg.version"

	rateLimitConfigStrategyPrefix    = "ratelimit.strategy."
	rateLimitStrategySqlMatchPattern = rateLimitConfigStrategyPrefix + "%"
)

// configuration tables
type conf struct {
	ID        uint32
	Name      string `gorm:"unique;size:128;not null"` // config name
	Value     string `gorm:"size:32698;not null"`      // config value
	CreatedAt time.Time
	UpdatedAt time.Time
}

func (conf) TableName() string {
	return "configs"
}

type confStore struct {
	*baseStore
}

func newConfStore(db *gorm.DB) *confStore {
	return &confStore{
		baseStore: newBaseStore(db),
	}
}

func (cs *confStore) LoadConfig(confNames ...string) (map[string]interface{}, error) {
	var confs []conf

	if err := cs.db.Where("name IN ?", confNames).Find(&confs).Error; err != nil {
		return nil, err
	}

	res := make(map[string]interface{}, len(confs))
	for _, c := range confs {
		res[c.Name] = c.Value
	}

	return res, nil
}

func (cs *confStore) StoreConfig(confName string, confVal interface{}) error {
	return cs.db.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "name"}},
		DoUpdates: clause.Assignments(map[string]interface{}{"value": confVal}),
	}).Create(&conf{
		Name:  confName,
		Value: confVal.(string),
	}).Error
}

func (cs *confStore) GetReorgVersion() (int, error) {
	var result conf
	exists, err := cs.exists(&result, "name = ?", MysqlConfKeyReorgVersion)
	if err != nil {
		return 0, err
	}

	if !exists {
		return 0, nil
	}

	return strconv.Atoi(result.Value)
}

// thread unsafe
func (cs *confStore) createOrUpdateReorgVersion(dbTx *gorm.DB) error {
	version, err := cs.GetReorgVersion()
	if err != nil {
		return err
	}

	newVersion := strconv.Itoa(version + 1)

	return cs.StoreConfig(MysqlConfKeyReorgVersion, newVersion)
}

// ratelimit config

func (cs *confStore) LoadRateLimitConfigs() *rate.Config {
	var cfgs []conf
	if err := cs.db.Where("name LIKE ?", rateLimitStrategySqlMatchPattern).Find(&cfgs).Error; err != nil {
		logrus.WithError(err).Error("Failed to load rate limit config from db")
		return nil
	}

	if len(cfgs) == 0 {
		return &rate.Config{}
	}

	var strategies []*rate.Strategy

	// load ratelimit strategies
	for _, v := range cfgs {
		strategy, err := cs.loadRateLimitStrategy(v)
		if err != nil {
			logrus.WithField("cfg", v).WithError(err).Warn("Invalid rate limit strategy config")
			continue
		}

		if strategy != nil {
			strategies = append(strategies, strategy)
		}
	}

	return &rate.Config{Strategies: strategies}
}

func (cs *confStore) loadRateLimitStrategy(cfg conf) (*rate.Strategy, error) {
	// eg., ratelimit.strategy.whitelist
	name := cfg.Name[len(rateLimitConfigStrategyPrefix):]
	if len(name) == 0 {
		return nil, errors.New("name is too short")
	}

	strategy := rate.Strategy{}
	data := []byte(cfg.Value)

	if err := json.Unmarshal(data, &strategy); err != nil {
		return nil, errors.WithMessage(err, "malformed json string")
	}

	strategy.ID = cfg.ID
	strategy.Name = name
	strategy.MD5 = md5.Sum(data)

	return &strategy, nil
}
