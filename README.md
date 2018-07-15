# pg-dump-upsert

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
$ go get github.com/tomyl/pg-dump-upsert
$ ~/go/bin/pg-dump-upsert -h
Usage of go/bin/pg-dump-upsert:
  -conflict-column string
        Append an ON CONFLICT clause for this column. All other columns will be included in a DO UPDATE SET list.
  -dsn string
        Connection string. Example: postgres://user:password@localhost:5432/db?sslmode=disable
  -insert string
        What columns to include in INSERT statement. Defaults to all columns
  -noconflict
        Append ON CONFLICT DO NOTHING.
  -table string
        Table to dump.
  -tx
        Wrap INSERT statements in transaction.
```

# Usage

Dump all rows in table `employee`:

```bash
$ pg-dump-upsert -dsn "postgres://user:password@host:5432/db" -table employee 
INSERT INTO source (id, created_at, name) VALUES (1, '2018-06-13 21:10:34.769555+08', 'Jane Doe');
...
```

Choose which columns to dump:

```bash
$ pg-dump-upsert -dsn "postgres://user:password@host:5432/db" -table employee -insert id,name
INSERT INTO source (id, name) VALUES (1, 'Jane Doe');
...
```

Ignore conflicts:

```bash
$ pg-dump-upsert -dsn "postgres://user:password@host:5432/db" -table employee -noconflict
INSERT INTO source (id, created_at, name) VALUES (1, '2018-06-13 21:10:34.769555+08', 'Jane Doe') ON CONFLICT DO NOTHING;
...
```

Update columns on conflict:

```bash
$ pg-dump-upsert -dsn "postgres://user:password@host:5432/db" -table employee -conflict-column id
INSERT INTO source (id, created_at, name) VALUES (1, '2018-06-13 21:10:34.769555+08', 'Jane Doe') ON CONFLICT (id) DO UPDATE SET created_at=EXCLUDED.created_at, name=EXCLUDED.name;
...
```

# TODO
- [ ] Allow which columns to update when specifying `-conflict-column`?
- [ ] Allow specify `SELECT` query or `WHERE` clause?
- [ ] Properly quote identifiers.
- [ ] Unit tests would be nice...
- [ ] Finish this TODO list.
