package mysql

import (
	"errors"

	"github.com/Conflux-Chain/confura/store"
	"gorm.io/gorm"
)

// baseStore provides basic store common operatition.
type baseStore struct {
	db *gorm.DB
}

func newBaseStore(db *gorm.DB) *baseStore {
	return &baseStore{db}
}

func (bs *baseStore) DB() *gorm.DB {
	return bs.db
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
