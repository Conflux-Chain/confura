package mysql

import (
	"fmt"
	"strconv"
	"strings"

	"gorm.io/gorm"
)

// mysqlPartition represents a MySQL table partition.
type mysqlPartition struct {
	Name            string `gorm:"column:PARTITION_NAME"`
	Description     string `gorm:"column:PARTITION_DESCRIPTION"`
	OrdinalPosition int    `gorm:"column:PARTITION_ORDINAL_POSITION"`
}

// mysqlPartitioner helper struct for MySQL partitioned tables.
type mysqlPartitioner struct {
	dbName    string // database name
	tableName string // table name
}

func newMysqlPartitioner(dbName, tableName string) *mysqlPartitioner {
	return &mysqlPartitioner{
		dbName: dbName, tableName: tableName,
	}
}

// latestPartition returns latest created partition of the table.
func (msp *mysqlPartitioner) latestPartition(db *gorm.DB) (*mysqlPartition, error) {
	partitions, err := msp.loadPartitions(db, false, 1)
	if err == nil && len(partitions) > 0 {
		return partitions[0], nil
	}

	return nil, err
}

// loadPartitions loads partitions of the table with specified ordinal position order.
func (msp *mysqlPartitioner) loadPartitions(db *gorm.DB, orderAsc bool, limit int) ([]*mysqlPartition, error) {
	db = db.Table("information_schema.partitions").
		Where("TABLE_SCHEMA = ?", msp.dbName).
		Where("TABLE_NAME = ?", msp.tableName).
		Where("PARTITION_NAME IS NOT NULL")

	if orderAsc {
		db = db.Order("PARTITION_ORDINAL_POSITION ASC")
	} else {
		db = db.Order("PARTITION_ORDINAL_POSITION DESC")
	}

	if limit > 0 {
		db = db.Limit(limit)
	}

	var partitions []*mysqlPartition
	if err := db.Find(&partitions).Error; err != nil {
		return nil, err
	}

	return partitions, nil
}

// mysqlRangePartitioner helper struct for MySQL range partitioned tables.
type mysqlRangePartitioner struct {
	*mysqlPartitioner

	rangeBy string
}

func newMysqlRangePartitioner(dbName, tableName, rangeBy string) *mysqlRangePartitioner {
	return &mysqlRangePartitioner{
		mysqlPartitioner: newMysqlPartitioner(dbName, tableName),
		rangeBy:          rangeBy,
	}
}

// convert converts non-partitioned table to range partitioned table.
func (mrp *mysqlRangePartitioner) convert(db *gorm.DB, initIndex int, threshold uint64) error {
	sql := fmt.Sprintf(
		"ALTER TABLE %v PARTITION BY RANGE (%v) (PARTITION %v VALUES LESS THAN (%v));",
		mrp.tableName,
		mrp.rangeBy,
		mrp.partitionName(initIndex),
		threshold,
	)
	return db.Exec(sql).Error
}

func (mrp *mysqlRangePartitioner) addPartition(db *gorm.DB, index int, threshold uint64) error {
	sql := fmt.Sprintf(
		"ALTER TABLE %v ADD PARTITION (PARTITION %v VALUES LESS THAN (%v));",
		mrp.tableName,
		mrp.partitionName(index),
		threshold,
	)
	return db.Exec(sql).Error
}

func (mrp *mysqlRangePartitioner) removePartition(db *gorm.DB, partiton *mysqlPartition) error {
	sql := fmt.Sprintf(
		"ALTER TABLE %v DROP PARTITION %v;",
		mrp.tableName,
		partiton.Name,
	)
	return db.Exec(sql).Error
}

func (*mysqlRangePartitioner) indexOfPartition(partition *mysqlPartition) int {
	v, err := strconv.Atoi(strings.TrimPrefix(partition.Name, "p"))
	if err == nil {
		return v
	}

	return -1
}

func (*mysqlRangePartitioner) partitionName(index int) string {
	return fmt.Sprintf("p%d", index)
}
