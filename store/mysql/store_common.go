package mysql

import (
	"errors"

	"github.com/conflux-chain/conflux-infura/store"
	"gorm.io/gorm"
)

type baseStore struct {
	db *gorm.DB
}

func newBaseStore(db *gorm.DB) *baseStore {
	return &baseStore{db}
}

func (baseStore) IsRecordNotFound(err error) bool {
	return errors.Is(err, gorm.ErrRecordNotFound) || errors.Is(err, store.ErrNotFound)
}

func (bs *baseStore) Close() error {
	if mysqlDb, err := bs.db.DB(); err != nil {
		return err
	} else {
		return mysqlDb.Close()
	}
}

func (bs *baseStore) exists(modelPtr interface{}, whereQuery string, args ...interface{}) (bool, error) {
	err := bs.db.Where(whereQuery, args...).First(modelPtr).Error
	if err == nil {
		return true, nil
	}

	if bs.IsRecordNotFound(err) {
		return false, nil
	}

	return false, err
}
