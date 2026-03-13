package mysql

import (
	"gorm.io/gorm"
)

// InternalContractLog stores virtual logs synthesized from internal contract traces.
type InternalContractLog struct {
	ID uint64 `gorm:"primaryKey;autoIncrement"`

	BlockNumber  uint64 `gorm:"column:bn;index:idx_addr_bn,priority:2;index:idx_addr_t0_bn,priority:3"`
	Epoch        uint64 `gorm:"index:idx_epoch;index:idx_addr_epoch,priority:2;index:idx_addr_t0_epoch,priority:3"`
	BlockHash    string `gorm:"column:bh;size:66;not null"`
	TxHash       string `gorm:"column:th;size:66;not null"`
	TxIndex      int    `gorm:"column:ti"`
	LogIndex     int    `gorm:"column:li"`
	AddressIndex uint8  `gorm:"column:address;index:idx_addr_bn,priority:1;index:idx_addr_t0_bn,priority:1;index:idx_addr_epoch,priority:1;index:idx_addr_t0_epoch,priority:1"`
	Topic0Index  uint8  `gorm:"column:topic0;index:idx_addr_t0_bn,priority:2;index:idx_addr_t0_epoch,priority:2"`
	Topic1       string `gorm:"size:66"`
	Topic2       string `gorm:"size:66"`
	Topic3       string `gorm:"size:66"`
	Data         []byte `gorm:"type:mediumBlob"`
}

func (InternalContractLog) TableName() string {
	return "internal_contract_logs"
}

type InternalContractLogStore struct {
	*baseStore
}

func NewInternalContractLogStore(db *gorm.DB) *InternalContractLogStore {
	return &InternalContractLogStore{baseStore: newBaseStore(db)}
}

// Pop removes all logs at or after the given epoch (for reorg handling).
func (s *InternalContractLogStore) Pop(dbTx *gorm.DB, epochFrom uint64) error {
	return dbTx.Where("epoch >= ?", epochFrom).Delete(&InternalContractLog{}).Error
}
