package pgdump_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/tomyl/pg-dump-upsert/internal/testdb"
	"github.com/tomyl/pg-dump-upsert/pgdump"
)

const (
	schema = `
create table employee (
    id integer primary key,
    created_at timestamptz not null default current_timestamp,
    name text not null,
    salary integer not null,
    camelCase1 text,
    "camelCase2" text,
    mytsvector tsvector,
    search tsvector GENERATED ALWAYS AS (to_tsvector('english', coalesce(name, '') )) STORED
);

create view myview as select * from employee where salary >= 100000;
`
	rows = `
insert into employee (id, name, salary, mytsvector) values (1, 'Alice', 123456, to_tsvector('english', 'The Fat Rats'));
insert into employee (id, name, salary) values (2, 'Bob', 90000);
`
)

func TestDump(t *testing.T) {
	// Connect to databases
	srcdb := testdb.New(t)
	dstdb := testdb.New(t)

	// Prepare source database
	_, err := srcdb.Exec(schema)
	require.NoError(t, err)

	_, err = srcdb.Exec(rows)
	require.NoError(t, err)

	// Prepare destination database
	_, err = dstdb.Exec(schema)
	require.NoError(t, err)

	var opts pgdump.Options
	var dump strings.Builder
	require.NoError(t, pgdump.DumpStream(&dump, pgdump.NewQuerier(srcdb), "employee", &opts))

	// Insert dumped rows
	_, err = dstdb.Exec(dump.String())
	require.NoError(t, err)
}
