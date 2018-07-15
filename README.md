# pg-dump-upsert

Simple tool to dump a Postgresql table as INSERT statements with ON CONFLICT clause.

**Pre-alpha software**. Expect crashes, data loss, silent data corruption etc.

# Rationale

TODO

# Usage

```bash
go get github/tomyl/pg-dump-upsert
```

TODO

# TODO
- [ ] Allow which columns to update when specifying `-conflict-column`?
- [ ] Allow specify `SELECT` query or `WHERE` clause?
- [ ] Finish this TODO list.
