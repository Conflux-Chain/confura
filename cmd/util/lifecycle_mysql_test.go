package util

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"testing"
	"time"

	driver "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
)

func TestMySQLShutdownWaitsForCommit(t *testing.T) {
	dsn := os.Getenv("CONFURA_TEST_MYSQL_DSN")
	if dsn == "" {
		t.Skip("CONFURA_TEST_MYSQL_DSN is required")
	}
	cfg, err := driver.ParseDSN(dsn)
	require.NoError(t, err)
	cfg.DBName = ""
	admin, err := sql.Open("mysql", cfg.FormatDSN())
	require.NoError(t, err)
	defer admin.Close()
	name := fmt.Sprintf("confura_shutdown_test_%d", time.Now().UnixNano())
	_, err = admin.Exec("CREATE DATABASE " + name)
	require.NoError(t, err)
	defer admin.Exec("DROP DATABASE " + name)
	cfg.DBName = name
	db, err := sql.Open("mysql", cfg.FormatDSN())
	require.NoError(t, err)
	defer db.Close()
	_, err = db.Exec("CREATE TABLE commits (id INT PRIMARY KEY)")
	require.NoError(t, err)
	life, err := NewLifecycle(context.Background())
	require.NoError(t, err)
	defer life.Close()
	tx, err := db.Begin()
	require.NoError(t, err)
	_, err = tx.Exec("INSERT INTO commits VALUES (1)")
	require.NoError(t, err)
	release, committed := make(chan struct{}), make(chan error, 1)
	life.Wg.Add(1)
	go func() { defer life.Wg.Done(); <-release; committed <- tx.Commit() }()
	closed := make(chan struct{})
	go func() { life.Shutdown(); db.Close(); close(closed) }()
	<-life.Ctx.Done()
	select {
	case <-closed:
		t.Fatal("database closed before writer returned")
	default:
	}
	require.NoError(t, db.Ping())
	close(release)
	require.NoError(t, <-committed)
	<-closed
	var count int
	require.NoError(t, admin.QueryRow("SELECT COUNT(*) FROM "+name+".commits").Scan(&count))
	require.Equal(t, 1, count)
}
