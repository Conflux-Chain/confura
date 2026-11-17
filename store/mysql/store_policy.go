package mysql

import (
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/Conflux-Chain/confura/util/rate"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

var ErrConfigChanged = errors.New("configuration changed; read and confirm it again")

// ConfigSnapshot identifies the exact configuration presented to an operator.
type ConfigSnapshot struct {
	ID    uint32
	Name  string
	Value string
}

func snapshotConfig(c conf) *ConfigSnapshot {
	return &ConfigSnapshot{ID: c.ID, Name: c.Name, Value: c.Value}
}

func (cs *confStore) LoadConfigSnapshot(name string) (*ConfigSnapshot, error) {
	var row conf
	err := cs.db.Where("name = ?", name).First(&row).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return snapshotConfig(row), nil
}

// StoreConfigIfUnchanged writes only the configuration the operator confirmed.
// A nil snapshot means that the name must still be absent.
func (cs *confStore) StoreConfigIfUnchanged(name, value string, expected *ConfigSnapshot) error {
	return cs.db.Transaction(func(tx *gorm.DB) error {
		row, err := lockConfig(tx, name, expected)
		if err != nil {
			return err
		}
		if expected == nil {
			if row != nil {
				return ErrConfigChanged
			}
			return tx.Create(&conf{Name: name, Value: value}).Error
		}
		return tx.Model(row).Update("value", value).Error
	})
}

func lockConfig(tx *gorm.DB, name string, expected *ConfigSnapshot) (*conf, error) {
	var row conf
	err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("name = ?", name).First(&row).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		if expected != nil {
			return nil, ErrConfigChanged
		}
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if expected != nil && *snapshotConfig(row) != *expected {
		return nil, ErrConfigChanged
	}
	return &row, nil
}

// DeleteConfigChecked protects policy references and optionally checks the
// configuration shown before confirmation. Key creation locks the same rows.
func (cs *confStore) DeleteConfigChecked(name string, expected *ConfigSnapshot) (removed bool, err error) {
	err = cs.db.Transaction(func(tx *gorm.DB) error {
		row, err := lockConfig(tx, name, expected)
		if err != nil || row == nil {
			return err
		}
		column := ""
		switch {
		case strings.HasPrefix(name, RateLimitStrategyConfKeyPrefix):
			column = "s_id"
		case strings.HasPrefix(name, AclAllowListConfKeyPrefix):
			column = "acl_id"
		}
		if column != "" {
			// Locking reads see keys committed while this transaction waited for
			// the policy lock, including under MySQL REPEATABLE READ.
			var refs []RateLimit
			if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Select("id").Where(column+" = ?", row.ID).Find(&refs).Error; err != nil {
				return err
			}
			if len(refs) > 0 {
				return fmt.Errorf("configuration is referenced by %d access keys", len(refs))
			}
		}
		res := tx.Delete(row)
		removed = res.RowsAffected != 0
		return res.Error
	})
	return removed && err == nil, err
}

func (rls *RateLimitStore) addRateLimitChecked(key *RateLimit, expected []*ConfigSnapshot) error {
	if key.SID == 0 {
		return errors.New("strategy ID must not be zero")
	}
	if rate.LimitType(key.LimitType) != rate.LimitTypeByKey && rate.LimitType(key.LimitType) != rate.LimitTypeByIp {
		return errors.New("invalid rate limit type")
	}
	return rls.db.Transaction(func(tx *gorm.DB) error {
		ids := []uint32{key.SID}
		if key.AclID != 0 && key.AclID != key.SID {
			ids = append(ids, key.AclID)
		}
		sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
		for _, id := range ids {
			var row conf
			if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).First(&row, id).Error; err != nil {
				return fmt.Errorf("load bound policy: %w", err)
			}
			if id == key.SID && !strings.HasPrefix(row.Name, RateLimitStrategyConfKeyPrefix) || id == key.AclID && !strings.HasPrefix(row.Name, AclAllowListConfKeyPrefix) {
				return errors.New("bound policy has an invalid type")
			}
			for _, want := range expected {
				if want != nil && want.ID == id && *snapshotConfig(row) != *want {
					return ErrConfigChanged
				}
			}
		}
		return tx.Create(key).Error
	})
}
