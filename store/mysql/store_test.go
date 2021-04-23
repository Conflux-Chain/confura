package mysql

import (
	"fmt"
	"testing"

	mocket "github.com/selvatico/go-mocket"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func setupMockStub() *gorm.DB {
	mocket.Catcher.Register()
	mocket.Catcher.Logging = true
	mocket.Catcher.PanicOnEmptyResponse = true

	setVersion()

	db, err := gorm.Open(mysql.New(mysql.Config{DriverName: mocket.DriverName, DSN: "mocket"}))
	if err != nil {
		logrus.WithError(err).Fatal("Failed to setup gorm db stub")
	}

	return db
}

func setVersion() {
	commonReply := []map[string]interface{}{{"VERSION()": "5.7.32-log"}}
	mocket.Catcher.Attach([]*mocket.FakeResponse{
		{
			Pattern:  "SELECT VERSION()",
			Response: commonReply,
			Once:     false,
		},
	})
}

func setEpochRange(t EpochDataType, minEpoch, maxEpoch uint64) {
	statement := fmt.Sprintf("SELECT MIN(epoch) AS min_epoch, MAX(epoch) AS max_epoch FROM %v", EpochDataTypeTableMap[t])
	commonReply := []map[string]interface{}{{"min_epoch": minEpoch, "max_epoch": maxEpoch}}
	mocket.Catcher.Attach([]*mocket.FakeResponse{
		{
			Pattern:  statement,
			Response: commonReply,
			Once:     false,
		},
	})
}

func setEpochTotal(t EpochDataType, total uint64) {
	statement := fmt.Sprintf("SELECT COUNT(*) AS total FROM %v", EpochDataTypeTableMap[t])
	commonReply := []map[string]interface{}{{"total": total}}
	mocket.Catcher.Attach([]*mocket.FakeResponse{
		{
			Pattern:  statement,
			Response: commonReply,
			Once:     false,
		},
	})
}

func TestLoadEpochRange(t *testing.T) {
	testCases := []struct {
		dataType EpochDataType
		minEpoch uint64
		maxEpoch uint64
		total    uint64
	}{
		{EpochBlock, 0, 1000, 4000},
		{EpochTransaction, 0, 1000, 40000},
		{EpochLog, 0, 1000, 400000},
	}

	// Setup testcases
	for _, tc := range testCases {
		setEpochRange(tc.dataType, tc.minEpoch, tc.maxEpoch)
		setEpochTotal(tc.dataType, tc.total)
	}

	// Initialize database store
	dbStub := setupMockStub()
	mysqlStore := mustNewStore(dbStub)

	var getEpochRange func() (uint64, uint64, error)
	for _, tc := range testCases {
		switch tc.dataType {
		case EpochBlock:
			getEpochRange = mysqlStore.GetBlockEpochRange
		case EpochTransaction:
			getEpochRange = mysqlStore.GetTransactionEpochRange
		case EpochLog:
			getEpochRange = mysqlStore.GetLogEpochRange
		}

		minEpoch, maxEpoch, err := getEpochRange()
		assert.Nil(t, err)
		assert.Equal(t, tc.minEpoch, minEpoch)
		assert.Equal(t, tc.maxEpoch, maxEpoch)

		total, err := mysqlStore.loadEpochTotal(tc.dataType)
		assert.Nil(t, err)
		assert.Equal(t, tc.total, total)
	}
}
