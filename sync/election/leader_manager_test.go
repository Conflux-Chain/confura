package election

import (
	"context"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Conflux-Chain/confura/util"
	"github.com/Conflux-Chain/go-conflux-util/dlock"
	"github.com/Conflux-Chain/go-conflux-util/store/mysql"
	"github.com/mcuadros/go-defaults"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"gorm.io/gorm"
)

var (
	db          *gorm.DB
	testMysqlBe *dlock.MySQLBackend

	elecKey    = "election.test"
	elecConfig = Config{
		Lease: 3 * time.Second,
		Retry: 500 * time.Millisecond,
		Renew: 1 * time.Second,
	}
)

// Please set the following environments before running tests:
// `TEST_DLOCK_MYSQL_HOST`: MySQL database host;
// `TEST_DLOCK_MYSQL_USER`: MySQL database username;
// `TEST_DLOCK_MYSQL_PWD`:  MySQL database password;
// `TEST_DLOCK_MYSQL_DB`:   MySQL database database.

func TestMain(m *testing.M) {
	if err := setup(); err != nil {
		panic(errors.WithMessage(err, "failed to setup"))
	}

	code := m.Run()

	if err := teardown(); err != nil {
		panic(errors.WithMessage(err, "failed to tear down"))
	}

	os.Exit(code)
}

func setup() error {
	host := os.Getenv("TEST_DLOCK_MYSQL_HOST")
	user := os.Getenv("TEST_DLOCK_MYSQL_USER")
	pwd := os.Getenv("TEST_DLOCK_MYSQL_PWD")
	dbn := os.Getenv("TEST_DLOCK_MYSQL_DB")

	if len(host) == 0 || len(dbn) == 0 {
		return nil
	}

	conf := mysql.Config{
		Host:     host,
		Database: dbn,
		Username: user,
		Password: pwd,
	}

	defaults.SetDefaults(&conf)
	db := conf.MustOpenOrCreate(&dlock.Dlock{})
	testMysqlBe = dlock.NewMySQLBackend(db)

	return nil
}

func teardown() error {
	if db != nil {
		db, err := db.DB()
		if err != nil {
			return err
		}

		return db.Close()
	}

	return nil
}

func TestLeaderManager(t *testing.T) {
	if testMysqlBe == nil {
		t.SkipNow()
	}

	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()

	start := int64(0)
	result := []int64{}

	wg := sync.WaitGroup{}
	mu := sync.Mutex{}

	dlm := dlock.NewLockManager(testMysqlBe)
	for i := 0; i < 50; i++ {
		conf := elecConfig
		conf.ID = fmt.Sprintf("leader%d", i)
		leaderMan := NewDlockLeaderManager(dlm, conf, elecKey)

		wg.Add(1)
		go func() {
			t.Logf("Leader election process #%d started", i)
			defer wg.Done()

			go leaderMan.Campaign(ctx)
			defer leaderMan.Stop()

			for leaderMan.Await(ctx) {
				select {
				case <-ctx.Done():
					return
				default:
					// drop out randomly (15% chance).
					if util.RandUint64(100) >= 85 {
						t.Logf("Leader election process #%d dropped out", i)
						return
					}

					val := atomic.LoadInt64(&start)

					// simulate heavy workload
					randomSleepT := time.Duration(util.RandUint64(1000))
					time.Sleep(randomSleepT * time.Millisecond)

					if err := leaderMan.Extend(ctx); err != nil {
						t.Logf("Failed to extend leadership %v with error: %v", conf.ID, err)
						continue
					}

					mu.Lock()
					for step := 0; step <= int(randomSleepT%100); step++ {
						result = append(result, val)
						val++
					}
					mu.Unlock()

					atomic.StoreInt64(&start, val)
				}
			}
		}()
	}

	wg.Wait()

	for i := 1; i < len(result); i++ {
		assert.Equalf(
			t, result[i-1]+1, result[i],
			"non-continuous integer at index:%v of result: %v", i, result,
		)
	}
}
