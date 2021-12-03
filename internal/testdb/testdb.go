package testdb

import (
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const env = "PGDUMP_TEST_DSN"

type DB struct {
	*sql.DB
	dsnurl *url.URL
	t      *testing.T
}

func (db *DB) TempDatabase() *sql.DB {
	dbname := fmt.Sprintf("pgdump_%d", time.Now().UnixNano())
	db.t.Logf("Creating database %q", dbname)
	_, err := db.Exec(`create database ` + dbname)
	require.NoError(db.t, err)

	db.t.Cleanup(func() {
		db.t.Logf("Dropping database %q", dbname)
		_, err = db.Exec(`drop database ` + dbname)
		require.NoError(db.t, err)
	})

	tempurl := *db.dsnurl
	tempurl.Path = "/" + dbname
	tempdsn := tempurl.String()

	tempdb, err := sql.Open("postgres", tempdsn)
	require.NoError(db.t, err)

	db.t.Cleanup(func() {
		require.NoError(db.t, tempdb.Close())
	})

	return tempdb
}

func New(t *testing.T) *DB {
	ownerdsn := os.Getenv(env)
	if ownerdsn == "" {
		t.Skipf("%s not set", env)
	}

	dsnurl, err := url.Parse(ownerdsn)
	require.NoError(t, err)

	ownerdb, err := sql.Open("postgres", ownerdsn)
	require.NoError(t, err)

	t.Cleanup(func() {
		require.NoError(t, ownerdb.Close())
	})

	return &DB{
		DB:     ownerdb,
		dsnurl: dsnurl,
		t:      t,
	}
}
