# pg-dump-upsert

Simple tool to dump a Postgresql table as INSERT statements with ON CONFLICT clause.

**Pre-alpha software**. Expect crashes, data loss, silent data corruption etc.

# Rationale

TODO

# Installation

```bash
$ go get github/tomyl/pg-dump-upsert
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
$ pg-dump-upsert -dsn "postgres://user:password@host:5432/db" -table employee -insert id,name -noconflict
INSERT INTO source (id, created_at, name) VALUES (1, '2018-06-13 21:10:34.769555+08', 'Jane Doe') ON CONFLICT DO NOTHING;
...
```

Update columns on conflict:

```bash
$ pg-dump-upsert -dsn "postgres://user:password@host:5432/db" -table employee -conflict-column id
INSERT INTO source (id, creted_at, name) VALUES (1, '2018-06-13 21:10:34.769555+08', 'Jane Doe') ON CONFLICT (id) DO UPDATE SET created_at=EXCLUDED.created_at, name=EXCLUDED.name;
...
```

# TODO
- [ ] Allow which columns to update when specifying `-conflict-column`?
- [ ] Allow specify `SELECT` query or `WHERE` clause?
- [ ] Finish this TODO list.
