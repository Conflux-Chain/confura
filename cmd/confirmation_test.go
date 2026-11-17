package cmd

import (
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/Conflux-Chain/confura/store/mysql"
	driver "github.com/go-sql-driver/mysql"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
	mysqldriver "gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func TestManagementConfirmation(t *testing.T) {
	if os.Getenv("CONFURA_CONFIRM_CHILD") == "1" {
		viper.Reset()
		viper.Set("store.mysql.enabled", true)
		viper.Set("store.mysql.dsn", os.Getenv("CONFURA_CONFIRM_DSN"))
		for i, arg := range os.Args {
			if arg == "--" {
				os.Args = append([]string{"confura"}, os.Args[i+1:]...)
				break
			}
		}
		Execute()
		return
	}
	dsn := os.Getenv("CONFURA_TEST_MYSQL_DSN")
	if dsn == "" {
		t.Skip("CONFURA_TEST_MYSQL_DSN is required")
	}
	cfg, err := driver.ParseDSN(dsn)
	require.NoError(t, err)
	cfg.DBName = ""
	admin, err := gorm.Open(mysqldriver.Open(cfg.FormatDSN()), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	require.NoError(t, err)
	name := fmt.Sprintf("confura_confirmation_test_%d", time.Now().UnixNano())
	require.NoError(t, admin.Exec("CREATE DATABASE "+name).Error)
	t.Cleanup(func() { admin.Exec("DROP DATABASE " + name); raw, _ := admin.DB(); raw.Close() })
	cfg.DBName = name
	db, err := gorm.Open(mysqldriver.Open(cfg.FormatDSN()), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	require.NoError(t, err)
	t.Cleanup(func() { raw, _ := db.DB(); raw.Close() })
	require.NoError(t, db.Exec("CREATE TABLE configs (id INT UNSIGNED PRIMARY KEY AUTO_INCREMENT, name VARCHAR(128) UNIQUE NOT NULL, value TEXT NOT NULL, created_at DATETIME(3), updated_at DATETIME(3))").Error)
	require.NoError(t, db.AutoMigrate(&mysql.RateLimit{}, &mysql.NodeRoute{}))
	for _, mode := range []string{"EOF", "Enter", "yes"} {
		for _, tc := range []struct {
			args  []string
			table string
			delta int64
		}{
			{[]string{"acl", "addal", "-a", "new", "-r", "{}"}, "configs", 1},
			{[]string{"acl", "rmal", "-a", "old"}, "configs", -1},
			{[]string{"ratelimit", "adds", "-a", "new", "-r", "{}"}, "configs", 1},
			{[]string{"ratelimit", "rms", "-a", "old"}, "configs", -1},
			{[]string{"ratelimit", "addk", "-s", "bound", "-t", "0", "-k", "0123456789abcdef0123456789abcdef"}, "ratelimits", 1},
			{[]string{"ratelimit", "rmk", "-k", "existing"}, "ratelimits", -1},
			{[]string{"noderoute", "add", "-k", "new", "-g", "group"}, "node_routes", 1},
			{[]string{"noderoute", "rm", "-k", "existing"}, "node_routes", -1},
		} {
			t.Run(mode+"/"+tc.args[0]+"/"+tc.args[1], func(t *testing.T) {
				for _, table := range []string{"ratelimits", "node_routes", "configs"} {
					require.NoError(t, db.Exec("DELETE FROM "+table).Error)
				}
				cs := mysql.NewCommonStores(db)
				for _, key := range []string{"acl.allowlist.old", "ratelimit.strategy.old", "ratelimit.strategy.bound"} {
					require.NoError(t, cs.StoreConfig(key, "{}"))
				}
				s, err := cs.LoadRateLimitStrategy("bound")
				require.NoError(t, err)
				require.NoError(t, cs.AddRateLimit(s.ID, 0, 0, "existing", ""))
				require.NoError(t, cs.AddNodeRoute("existing", "group"))
				var before, after int64
				require.NoError(t, db.Table(tc.table).Count(&before).Error)
				args := append([]string{"-test.run=^TestManagementConfirmation$", "--"}, tc.args...)
				args = append(args, "-n", "cfx")
				if mode == "yes" {
					args = append(args, "--yes")
				}
				child := exec.Command(os.Args[0], args...)
				child.Env = append(os.Environ(), "CONFURA_CONFIRM_CHILD=1", "CONFURA_CONFIRM_DSN="+cfg.FormatDSN())
				if mode == "Enter" {
					child.Stdin = strings.NewReader("\n")
				}
				out, err := child.CombinedOutput()
				if mode == "EOF" {
					require.Error(t, err, string(out))
					require.Contains(t, string(out), "confirmation not received")
				} else {
					require.NoError(t, err, string(out))
				}
				require.Contains(t, string(out), "Network: cfx")
				require.NoError(t, db.Table(tc.table).Count(&after).Error)
				if mode == "EOF" {
					require.Equal(t, before, after)
				} else {
					require.Equal(t, before+tc.delta, after)
				}
			})
		}
	}
}
