package mysql

import (
	"sort"

	"github.com/pkg/errors"
	"gorm.io/gorm/clause"
)

// PreparePartition creates a write partition. The session serializes calls for
// its fid. Record both possible DDL names before allocation: MySQL DDL does not
// roll back when the subsequent metadata insert fails or its result is lost.
func (vfls *VirtualFilterLogStore) PreparePartition(fid string) (bnPartition, bool, error) {
	entity, tabler := vfls.filterEntity(fid), vfls.filterTabler(fid)
	latest, exists, err := vfls.latestPartition(entity)
	if err != nil {
		return bnPartition{}, false, err
	}
	if exists && latest.Count < bnPartitionedLogVolumeSize {
		return *latest, false, nil
	}
	part := bnPartition{Entity: entity}
	if exists {
		part.Index = latest.Index + 1
	}
	table := vfls.getPartitionedTableName(tabler, part.Index)
	vfls.pendingTables.Store(fid, []string{entity, table})
	if _, err = vfls.createPartitionedTable(vfls.db, tabler, part.Index); err != nil {
		return part, false, errors.WithMessage(err, "failed to create virtual filter partition")
	}
	if err = vfls.db.Create(&part).Error; err != nil {
		return part, false, errors.WithMessage(err, "failed to record virtual filter partition")
	}
	// Metadata owns the final table from this point on.
	vfls.pendingTables.Delete(fid)
	return part, true, nil
}

// DeletePartitions includes allocations without metadata. Failed DROP or
// metadata deletion keeps ownership for another attempt by the closing session.
func (vfls *VirtualFilterLogStore) DeletePartitions(fid string) error {
	entity, tabler := vfls.filterEntity(fid), vfls.filterTabler(fid)
	var parts []bnPartition
	if err := vfls.db.Where("entity = ?", entity).Find(&parts).Error; err != nil {
		return err
	}
	targets := make(map[string]bool, len(parts)+2)
	for _, part := range parts {
		targets[vfls.getPartitionedTableName(tabler, part.Index)] = true
	}
	if pending, ok := vfls.pendingTables.Load(fid); ok {
		for _, table := range pending.([]string) {
			targets[table] = true
		}
	}
	names := make([]string, 0, len(targets))
	for name := range targets {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		if err := vfls.dropFilterTable(name); err != nil {
			return err
		}
	}
	if err := vfls.db.Where("entity = ?", entity).Delete(&bnPartition{}).Error; err != nil {
		return err
	}
	vfls.pendingTables.Delete(fid)
	return nil
}

// GC drops archive tables before deleting their identities. A failure closes
// the publishing session, whose cleanup can then retry using the metadata.
// This ordering is specific to virtual filters; their worker excludes readers
// during publication and invalidates the session before exposing a failed GC.
func (vfls *VirtualFilterLogStore) GC(fid string) error {
	entity, tabler := vfls.filterEntity(fid), vfls.filterTabler(fid)
	var parts []bnPartition
	if err := vfls.db.Where("entity = ?", entity).Order("pi ASC").Find(&parts).Error; err != nil {
		return err
	}
	keep := int(maxArchiveVirtualFilterLogPartitions) + 1
	for i := 0; i < len(parts)-keep; i++ {
		part := parts[i]
		if err := vfls.dropFilterTable(vfls.getPartitionedTableName(tabler, part.Index)); err != nil {
			return err
		}
		if err := vfls.db.Delete(&part).Error; err != nil {
			return err
		}
	}
	return nil
}

func (vfls *VirtualFilterLogStore) dropFilterTable(name string) error {
	// Avoid HasTable: its bool result can hide a database/context error. An
	// idempotent DROP reports errors while tolerating an already removed table.
	return vfls.db.Exec("DROP TABLE IF EXISTS ?", clause.Table{Name: name}).Error
}
