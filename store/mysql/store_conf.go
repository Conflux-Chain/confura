package mysql

import (
	"strconv"
	"strings"
	"time"

	"github.com/conflux-chain/conflux-infura/util/rate"
	"github.com/sirupsen/logrus"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const (
	MysqlConfKeyReorgVersion = "reorg.version"
)

// conf configuration tables
type conf struct {
	ID        uint32
	Name      string `gorm:"unique;size:128;not null"` // config name
	Value     string `gorm:"size:256;not null"`        // config value
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

func (cs *confStore) LoadRateLimitConfigs() *rate.Config {
	var cfgs []conf
	if err := cs.db.Where("name LIKE ?", "ratelimit.%").Find(&cfgs).Error; err != nil {
		logrus.WithError(err).Error("Failed to load rate limit config from db")
		return nil
	}

	if len(cfgs) == 0 {
		return &rate.Config{}
	}

	rconf := &rate.Config{
		IpLimitOpts: make(map[string]rate.Option),
		WhiteList:   make(map[string]struct{}),
	}

	namePrefixLen := len("ratelimit.")

	for _, v := range cfgs {
		name := v.Name[namePrefixLen:]
		if len(name) == 0 {
			logrus.WithField("cfg", v).Warn("Invalid rate limit config, name is too short")
			continue
		}

		if strings.EqualFold(name, "whitelist") { // add white list
			whiteList := strings.Split(v.Value, ",")
			for _, name := range whiteList {
				rconf.WhiteList[name] = struct{}{}
			}
			continue
		}

		fields := strings.Split(v.Value, ",")
		if len(fields) != 2 {
			logrus.WithField("cfg", v).Warn("Invalid rate limit config, value fields mismatch")
		}

		r, err := strconv.Atoi(fields[0])
		if err != nil {
			logrus.WithField("cfg", v).Warn("Invalid rate limit config, rate is not number")
		}

		burst, err := strconv.Atoi(fields[1])
		if err != nil {
			logrus.WithField("cfg", v).Warn("Invalid rate limit config, burst is not number")
		}

		rconf.IpLimitOpts[name] = rate.NewOption(r, burst)
	}

	return rconf
}
