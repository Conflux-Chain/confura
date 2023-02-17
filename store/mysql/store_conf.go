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

	// pre-defined strategy config key prefix
	RateLimitStrategyConfKeyPrefix   = "ratelimit.strategy."
	rateLimitStrategySqlMatchPattern = RateLimitStrategyConfKeyPrefix + "%"

	// pre-defined node route group config key prefix
	NodeRouteGroupConfKeyPrefix   = "noderoute.group."
	nodeRouteGroupSqlMatchPattern = NodeRouteGroupConfKeyPrefix + "%"
)

// configuration tables
type conf struct {
	ID        uint32
	Name      string `gorm:"unique;size:128;not null"` // config name
	Value     string `gorm:"size:16250;not null"`      // config value
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

func (cs *confStore) DeleteConfig(confName string) (bool, error) {
	res := cs.db.Delete(&conf{}, "name = ?", confName)
	return res.RowsAffected > 0, res.Error
}

// reorg config

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

func (cs *confStore) LoadRateLimitStrategy(name string) (*rate.Strategy, error) {
	var cfg conf
	if err := cs.db.Where("name = ?", RateLimitStrategyConfKeyPrefix+name).First(&cfg).Error; err != nil {
		return nil, err
	}

	return cs.decodeRateLimitStrategy(cfg)
}

func (cs *confStore) LoadRateLimitConfigs() (*rate.Config, error) {
	var cfgs []conf
	if err := cs.db.Where("name LIKE ?", rateLimitStrategySqlMatchPattern).Find(&cfgs).Error; err != nil {
		return nil, err
	}

	if len(cfgs) == 0 {
		return &rate.Config{}, nil
	}

	strategies := make(map[uint32]*rate.Strategy)

	// decode ratelimit strategy from config item
	for _, v := range cfgs {
		strategy, err := cs.decodeRateLimitStrategy(v)
		if err != nil {
			logrus.WithField("cfg", v).WithError(err).Warn("Invalid rate limit strategy config")
			continue
		}

		strategies[v.ID] = strategy
	}

	return &rate.Config{Strategies: strategies}, nil
}

func (cs *confStore) decodeRateLimitStrategy(cfg conf) (*rate.Strategy, error) {
	// eg., ratelimit.strategy.whitelist
	name := cfg.Name[len(RateLimitStrategyConfKeyPrefix):]
	if len(name) == 0 {
		return nil, errors.New("strategy name is too short")
	}

	data := []byte(cfg.Value)
	stg := rate.NewStrategy(cfg.ID, name)

	if err := json.Unmarshal(data, stg); err != nil {
		return nil, err
	}

	stg.MD5 = md5.Sum(data) // calculate fingerprint
	return stg, nil
}

// node route config

type NodeRouteGroup struct {
	ID    uint32         // group ID
	Name  string         `json:"-"` // group name
	Nodes []string       // node urls
	MD5   [md5.Size]byte `json:"-"` // config data fingerprint
}

func (cs *confStore) LoadNodeRouteGroups() (map[uint32]*NodeRouteGroup, error) {
	var cfgs []conf
	if err := cs.db.Where("name LIKE ?", nodeRouteGroupSqlMatchPattern).Find(&cfgs).Error; err != nil {
		return nil, err
	}

	if len(cfgs) == 0 {
		return nil, nil
	}

	routeGroups := make(map[uint32]*NodeRouteGroup)

	// decode node route group from config item
	for _, v := range cfgs {
		grp, err := cs.decodeNodeRouteGroup(v)
		if err != nil {
			logrus.WithField("cfg", v).WithError(err).Warn("Invalid node route config")
			continue
		}

		routeGroups[grp.ID] = grp
	}

	return routeGroups, nil
}

func (cs *confStore) decodeNodeRouteGroup(cfg conf) (*NodeRouteGroup, error) {
	// eg., noderoute.group.cfxvip
	name := cfg.Name[len(NodeRouteGroupConfKeyPrefix):]
	if len(name) == 0 {
		return nil, errors.New("route group name is too short")
	}

	grp := NodeRouteGroup{ID: cfg.ID, Name: name}
	data := []byte(cfg.Value)

	if err := json.Unmarshal(data, &grp); err != nil {
		return nil, err
	}

	grp.MD5 = md5.Sum(data) // calculate fingerprint
	return &grp, nil
}
