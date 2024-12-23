package pgdump_test

import (
	"strings"
	"testing"

	"github.com/georgysavva/scany/sqlscan"
	"github.com/stretchr/testify/require"
	"golang.org/x/net/context"

	"github.com/tomyl/pg-dump-upsert/internal/testdb"
	"github.com/tomyl/pg-dump-upsert/pgdump"
)

const (
	schemaWithGeneratedColumn = `
create table mytable (
    myid integer primary key,
    mytimestamptz timestamptz not null default current_timestamp,
    mytext text not null,
    myinteger integer not null,
    myfloat double precision not null,
    camelCase1 text,
    "camelCase2" text,
    mytsvector tsvector,
    search tsvector GENERATED ALWAYS AS (to_tsvector('english', coalesce(mytext, '') )) STORED
);
`

	schemaWithoutGeneratedColumn = `
create table mytable (
    myid integer primary key,
    mytimestamptz timestamptz not null default current_timestamp,
    mytext text not null,
    myinteger integer not null,
    myfloat double precision not null,
    camelCase1 text,
    "camelCase2" text,
    mytsvector tsvector
);
`

	rows = `
insert into mytable (myid, mytext, myinteger, myfloat, mytsvector) values (1, 'Alice', 123456, 1234567.89, to_tsvector('english', 'The Fat Rats'));
insert into mytable (myid, mytext, myinteger, myfloat) values (2, 'Bob', 90000, -1);
`
)

func TestDumpKeepTableName(t *testing.T) {
	// Connect to databases
	ownerdb := testdb.New(t)
	srcdb := ownerdb.TempDatabase()
	dstdb := ownerdb.TempDatabase()

	// Prepare source database
	_, err := srcdb.Exec(schemaWithGeneratedColumn)
	require.NoError(t, err)

	_, err = srcdb.Exec(rows)
	require.NoError(t, err)

	// Prepare destination database
	_, err = dstdb.Exec(schemaWithGeneratedColumn)
	require.NoError(t, err)

	var opts pgdump.Options
	var dump strings.Builder
	require.NoError(t, pgdump.DumpStream(&dump, pgdump.NewQuerier(srcdb), "mytable", &opts))

	// Insert dumped rows
	_, err = dstdb.Exec(dump.String())
	require.NoError(t, err)
}

func TestDumpNewTableName(t *testing.T) {
	// Connect to databases
	ownerdb := testdb.New(t)
	tempdb := ownerdb.TempDatabase()

	// Prepare source table
	_, err := tempdb.Exec(schemaWithGeneratedColumn)
	require.NoError(t, err)

	_, err = tempdb.Exec(rows)
	require.NoError(t, err)

	// Prepare dest table
	_, err = tempdb.Exec(strings.ReplaceAll(schemaWithGeneratedColumn, "mytable", "mytable2"))
	require.NoError(t, err)

	var opts pgdump.Options
	opts.InsertTable = "mytable2"

	var dump strings.Builder
	require.NoError(t, pgdump.DumpStream(&dump, pgdump.NewQuerier(tempdb), "mytable", &opts))

	// Insert dumped rows
	_, err = tempdb.Exec(dump.String())
	require.NoError(t, err)

	// Make sure the tables are identical
	var ids []int
	require.NoError(t, sqlscan.Select(context.Background(), tempdb, &ids, `select myid from (select * from mytable except select * from mytable2) ss`))
	require.Equal(t, 0, len(ids))
}

func TestDumpView(t *testing.T) {
	// Connect to databases
	ownerdb := testdb.New(t)
	tempdb := ownerdb.TempDatabase()

	// Prepare source table
	_, err := tempdb.Exec(schemaWithoutGeneratedColumn)
	require.NoError(t, err)

	_, err = tempdb.Exec(rows)
	require.NoError(t, err)

	// Add a view
	_, err = tempdb.Exec(`create view myview as select * from mytable where myinteger >= 100000`)
	require.NoError(t, err)

	// Prepare dest table
	_, err = tempdb.Exec(strings.ReplaceAll(schemaWithoutGeneratedColumn, "mytable", "mytable2"))
	require.NoError(t, err)

	var opts pgdump.Options
	opts.InsertTable = "mytable2"

	var dump strings.Builder
	require.NoError(t, pgdump.DumpStream(&dump, pgdump.NewQuerier(tempdb), "myview", &opts))

	// Insert dumped rows
	_, err = tempdb.Exec(dump.String())
	require.NoError(t, err)

	// Make sure we got the expected dump
	var ids []int
	require.NoError(t, sqlscan.Select(context.Background(), tempdb, &ids, `select myid from (select * from mytable except select * from mytable2) ss`))
	require.Equal(t, 1, len(ids))
	require.Equal(t, 2, ids[0])
}
