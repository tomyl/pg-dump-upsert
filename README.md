# pg-dump-upsert :elephant::poop:

![CI](https://github.com/tomyl/pg-dump-upsert/actions/workflows/ci.yml/badge.svg?branch=master&event=push)
[![GoDoc](https://godoc.org/github.com/tomyl/pg-dump-upsert/pgdump?status.png)](http://godoc.org/github.com/tomyl/pg-dump-upsert/pgdump)
[![Go Report Card](https://goreportcard.com/badge/github.com/tomyl/pg-dump-upsert)](https://goreportcard.com/report/github.com/tomyl/pg-dump-upsert)

Simple tool to dump a Postgresql table as `INSERT` statements with `ON
CONFLICT` clause (also known as "upsert" statements).

**Pre-alpha software**. Expect crashes, data loss, silent data corruption etc.

# Rationale

The [pg\_dump](https://www.postgresql.org/docs/current/static/app-pgdump.html)
command can dump tables as `INSERT` statements however you can't directly
restore such dumps if the database has conflicting rows. Furthermore `pg_dump`
is doing more work than simply querying the data and this sometimes causes
seemingly unrelated failures.

# Installation

```bash
$ go install github.com/tomyl/pg-dump-upsert@latest
$ pg-dump-upsert -h
Usage of pg-dump-upsert:
  -conflict-column string
        Append an ON CONFLICT clause for this column. All other columns will be included in a DO UPDATE SET list.
  -dsn string
        Connection string. Example: postgres://user:password@host:5432/db
  -from string
        Source table to dump.
  -insert-columns string
        Comma-separated list of columns to include in INSERT statement. Defaults to all columns.
  -noconflict
        Append ON CONFLICT DO NOTHING.
  -query string
        Use custom SELECT query. By default fetches all rows. Note that column order must match -insert-columns. It is also valid to just specify a WHERE clause. It will be appended to the default query.
  -to string
        Table name to use in INSERT statements. Defaults to the source table.
  -tx   
        Wrap INSERT statements in transaction.
  -verbose
        Log query statement to stderr.
```

# Examples

Dump all rows in table `employee`:

```bash
$ pg-dump-upsert -dsn "postgres://user:password@host:5432/db" -from employee 
INSERT INTO employee (id, created_at, name, salary) VALUES (1, '2018-06-13 21:10:34.769555+08', 'Jane Doe', 123456);
...
```

Choose which columns to dump:

```bash
$ pg-dump-upsert -dsn "postgres://user:password@host:5432/db" -from employee -insert-columns id,name
INSERT INTO employee (id, name) VALUES (1, 'Jane Doe');
...
```

Ignore conflicts:

```bash
$ pg-dump-upsert -dsn "postgres://user:password@host:5432/db" -from employee -noconflict
INSERT INTO employee (id, created_at, name, salary) VALUES (1, '2018-06-13 21:10:34.769555+08', 'Jane Doe' 123456) ON CONFLICT DO NOTHING;
...
```

Update columns on conflict:

```bash
$ pg-dump-upsert -dsn "postgres://user:password@host:5432/db" -from employee -conflict-column id
INSERT INTO employee (id, created_at, name, salary) VALUES (1, '2018-06-13 21:10:34.769555+08', 'Jane Doe', 123456) ON CONFLICT (id) DO UPDATE SET created_at=EXCLUDED.created_at, name=EXCLUDED.name;
...
```

Fetch a subset of the rows:

```bash
$ pg-dump-upsert -dsn "postgres://user:password@host:5432/db" -from employee -query "WHERE salary > 12345"
INSERT INTO employee (id, created_at, name, salary) VALUES (1, '2018-06-13 21:10:34.769555+08', 'Jane Doe', 123456);
...
```

Use a different table name in the `INSERT` statements:

```bash
$ pg-dump-upsert -dsn "postgres://user:password@host:5432/db" -from employee -to minions
INSERT INTO minions (id, created_at, name, salary) VALUES (1, '2018-06-13 21:10:34.769555+08', 'Jane Doe', 123456);
...
```

To restore a dump, simply use the `\i` command in `psql`.

# TODO
- [ ] Implement support for all Postgres data types.
- [ ] Skip generated columns when dumping views.
- [ ] Allow which columns to update when specifying `-conflict-column`?
- [ ] More unit tests would be nice...
- [ ] Finish this TODO list.
