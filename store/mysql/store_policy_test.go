package mysql

import (
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/Conflux-Chain/confura/util/rate"
	driver "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
	mysqldriver "gorm.io/driver/mysql"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func policyTestDB(t *testing.T, mysql bool) *gorm.DB {
	t.Helper()
	var dialect gorm.Dialector = sqlite.Open(":memory:")
	if mysql {
		dsn := os.Getenv("CONFURA_TEST_MYSQL_DSN")
		if dsn == "" {
			t.Skip("CONFURA_TEST_MYSQL_DSN is required for MySQL locking tests")
		}
		cfg, err := driver.ParseDSN(dsn)
		require.NoError(t, err)
		cfg.DBName = ""
		admin, err := gorm.Open(mysqldriver.Open(cfg.FormatDSN()), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
		require.NoError(t, err)
		name := fmt.Sprintf("confura_policy_test_%d", time.Now().UnixNano())
		require.NoError(t, admin.Exec("CREATE DATABASE "+name).Error)
		t.Cleanup(func() { admin.Exec("DROP DATABASE " + name); db, _ := admin.DB(); db.Close() })
		cfg.DBName = name
		dialect = mysqldriver.Open(cfg.FormatDSN())
	}
	db, err := gorm.Open(dialect, &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	require.NoError(t, err)
	raw, err := db.DB()
	require.NoError(t, err)
	t.Cleanup(func() { raw.Close() })
	if !mysql {
		raw.SetMaxOpenConns(1)
	}
	require.NoError(t, db.AutoMigrate(&conf{}, &RateLimit{}))
	return db
}

func TestPolicyReferencesAndConfirmation(t *testing.T) {
	db := policyTestDB(t, false)
	cs, keys := newConfStore(db), NewRateLimitStore(db)
	require.NoError(t, cs.StoreConfig(RateLimitStrategyConfKeyPrefix+"limited", "{}"))
	require.NoError(t, cs.StoreConfig(AclAllowListConfKeyPrefix+"restricted", "{}"))
	s, err := cs.LoadConfigSnapshot(RateLimitStrategyConfKeyPrefix + "limited")
	require.NoError(t, err)
	a, err := cs.LoadConfigSnapshot(AclAllowListConfKeyPrefix + "restricted")
	require.NoError(t, err)
	require.Error(t, keys.AddRateLimit(0, 0, rate.LimitTypeByKey, "invalid", ""))
	require.Error(t, keys.AddRateLimit(a.ID, 0, rate.LimitTypeByKey, "wrongtype", ""))
	require.NoError(t, keys.AddRateLimit(s.ID, a.ID, rate.LimitTypeByKey, "fixture", "", s, a))
	for _, snapshot := range []*ConfigSnapshot{s, a} {
		removed, err := cs.DeleteConfigChecked(snapshot.Name, snapshot)
		require.ErrorContains(t, err, "referenced by 1")
		require.False(t, removed)
	}
	_, err = keys.DeleteRateLimit("fixture")
	require.NoError(t, err)
	require.NoError(t, cs.StoreConfig(s.Name, `{"rpc":{"algo":"token_bucket","option":{"rate":1,"burst":1}}}`))
	_, err = cs.DeleteConfigChecked(s.Name, s)
	require.ErrorIs(t, err, ErrConfigChanged)
	require.ErrorIs(t, keys.AddRateLimit(s.ID, 0, rate.LimitTypeByKey, "stale", "", s), ErrConfigChanged)
	removed, err := cs.DeleteConfigChecked(a.Name, a)
	require.NoError(t, err)
	require.True(t, removed)
	require.Error(t, keys.AddRateLimit(s.ID, a.ID, rate.LimitTypeByKey, "dangling", ""))
}

func TestMySQLConcurrentPolicyDeletionAndKeyCreation(t *testing.T) {
	db := policyTestDB(t, true)
	cs, keys := newConfStore(db), NewRateLimitStore(db)
	for i := 0; i < 20; i++ {
		name := fmt.Sprintf("%sconcurrent_%d", RateLimitStrategyConfKeyPrefix, i)
		require.NoError(t, cs.StoreConfig(name, "{}"))
		snapshot, err := cs.LoadConfigSnapshot(name)
		require.NoError(t, err)
		var addErr, deleteErr error
		var removed bool
		var wg sync.WaitGroup
		start := make(chan struct{})
		wg.Add(2)
		go func() {
			defer wg.Done()
			<-start
			addErr = keys.AddRateLimit(snapshot.ID, 0, rate.LimitTypeByKey, fmt.Sprintf("key%d", i), "", snapshot)
		}()
		go func() { defer wg.Done(); <-start; removed, deleteErr = cs.DeleteConfigChecked(name, snapshot) }()
		close(start)
		wg.Wait()
		if addErr == nil {
			require.Error(t, deleteErr)
			require.False(t, removed)
		} else {
			require.NoError(t, deleteErr)
			require.True(t, removed)
		}
		var dangling int64
		require.NoError(t, db.Raw("SELECT COUNT(*) FROM ratelimits r LEFT JOIN configs c ON r.s_id = c.id WHERE c.id IS NULL").Scan(&dangling).Error)
		require.Zero(t, dangling)
	}
}

func TestConfirmedObjectsCannotBeReplaced(t *testing.T) {
	db := policyTestDB(t, false)
	cs := newConfStore(db)
	require.NoError(t, cs.StoreConfigIfUnchanged("example", "first", nil))
	snapshot, err := cs.LoadConfigSnapshot("example")
	require.NoError(t, err)
	require.ErrorIs(t, cs.StoreConfigIfUnchanged("example", "second", nil), ErrConfigChanged)
	require.NoError(t, cs.StoreConfigIfUnchanged("example", "second", snapshot))
	require.ErrorIs(t, cs.StoreConfigIfUnchanged("example", "third", snapshot), ErrConfigChanged)
	require.NoError(t, db.AutoMigrate(&NodeRoute{}))
	routes := NewNodeRouteStore(db)
	require.NoError(t, routes.AddNodeRoute("route", "old"))
	route, err := routes.FindNodeRoute("route")
	require.NoError(t, err)
	require.NoError(t, db.Model(&NodeRoute{}).Where("id = ?", route.ID).Update("group", "new").Error)
	_, err = routes.DeleteNodeRouteChecked(route)
	require.ErrorIs(t, err, ErrConfigChanged)
	key := &RateLimit{SID: 1, LimitKey: "key"}
	require.NoError(t, db.Create(key).Error)
	require.NoError(t, db.Model(&RateLimit{}).Where("id = ?", key.ID).Update("s_id", 2).Error)
	_, err = NewRateLimitStore(db).DeleteRateLimitChecked(key)
	require.ErrorIs(t, err, ErrConfigChanged)
}

func TestInvalidSecurityRecordFailsWholeLoad(t *testing.T) {
	db := policyTestDB(t, false)
	cs := newConfStore(db)
	require.NoError(t, cs.StoreConfig(RateLimitStrategyConfKeyPrefix+"default", "{}"))
	require.NoError(t, cs.StoreConfig(AclAllowListConfKeyPrefix+"default", `{"ID":999,"Name":"other","Origins":["https://example.com"]}`))
	cfg, err := cs.LoadRateLimitConfigs()
	require.NoError(t, err)
	for id, al := range cfg.AllowLists {
		require.Equal(t, id, al.ID)
		require.Equal(t, "default", al.Name)
	}
	for _, prefix := range []string{RateLimitStrategyConfKeyPrefix, AclAllowListConfKeyPrefix} {
		require.NoError(t, cs.StoreConfig(prefix+"invalid", "null"))
		cfg, err = cs.LoadRateLimitConfigs()
		require.Error(t, err)
		require.Nil(t, cfg)
		_, err = cs.DeleteConfig(prefix + "invalid")
		require.NoError(t, err)
	}
}
