package mysql

import (
	"fmt"
	"hash/fnv"

	"github.com/Conflux-Chain/go-conflux-sdk/types/cfxaddress"
	"gorm.io/gorm"
	"gorm.io/gorm/schema"
)

type partitionedStore struct{}

func (*partitionedStore) getPartitionedTableName(tabler schema.Tabler, partition uint32) string {
	return fmt.Sprintf("%v_%v", tabler.TableName(), partition)
}

func (ps *partitionedStore) createPartitionedTable(db *gorm.DB, modelPtr schema.Tabler, partition uint32) (bool, error) {
	tableName := ps.getPartitionedTableName(modelPtr, partition)
	migrator := db.Migrator()

	if migrator.HasTable(tableName) {
		return false, nil
	}

	if err := migrator.CreateTable(modelPtr); err != nil {
		return false, err
	}

	// gorm do not support dynamic table name, so rename to create partitioned tables.
	if err := migrator.RenameTable(modelPtr.TableName(), tableName); err != nil {
		return false, err
	}

	return true, nil
}

func (ps *partitionedStore) createPartitionedTables(db *gorm.DB, modelPtr schema.Tabler, partitionFrom, count uint32) (int, error) {
	var numCreated int

	for i, end := partitionFrom, partitionFrom+count; i < end; i++ {
		created, err := ps.createPartitionedTable(db, modelPtr, i)
		if err != nil {
			return numCreated, err
		}

		if created {
			numCreated++
		}
	}

	return numCreated, nil
}

// func (ps *partitionedStore) deletePartitionedTable(db *gorm.DB, modelPtr schema.Tabler, partition uint32) (bool, error) {
// 	tableName := ps.getPartitionedTableName(modelPtr, partition)
// 	migrator := db.Migrator()

// 	if !migrator.HasTable(tableName) {
// 		return false, nil
// 	}

// 	if err := migrator.DropTable(tableName); err != nil {
// 		return false, err
// 	}

// 	return true, nil
// }

func (*partitionedStore) getPartitionByAddress(contract *cfxaddress.Address, totalPartitions uint32) uint32 {
	hasher := fnv.New32()
	hasher.Write(contract.MustGetCommonAddress().Bytes())
	// Use consistent hashing if repartition supported
	return hasher.Sum32() % totalPartitions
}
