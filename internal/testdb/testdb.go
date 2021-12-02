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

func New(t *testing.T) *sql.DB {
	ownerdsn := os.Getenv(env)
	if ownerdsn == "" {
		t.Skipf("%s not set", env)
	}

	dburl, err := url.Parse(ownerdsn)
	require.NoError(t, err)

	ownerdb, err := sql.Open("postgres", ownerdsn)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, ownerdb.Close())
	})

	dbname := fmt.Sprintf("pgdump_%d", time.Now().UnixNano())
	t.Logf("Creating database %q", dbname)
	_, err = ownerdb.Exec(`create database ` + dbname)
	require.NoError(t, err)

	t.Cleanup(func() {
		t.Logf("Dropping database %q", dbname)
		_, err = ownerdb.Exec(`drop database ` + dbname)
		require.NoError(t, err)
	})

	dburl.Path = "/" + dbname
	testdsn := dburl.String()

	testdb, err := sql.Open("postgres", testdsn)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, testdb.Close())
	})

	return testdb
}
