# pg\_unprivileged\_replication

Service capable of performing row-level replication a PostgreSQL database without superuser access on either the
leading or following database.

Like the main project, this is **pre-alpha software**.
Expect crashes, data loss, silent data corruption etc.

> It worked for me though to replicate a &gt;500GiB database with 2.5k commits per second in less than 24 hours with no
> perceivable performance impact.
> Your mileage _will_ vary.

## How it works

`pg_unprivileged_replication` scans tables on the leader (sender) for new and modified rows (via `id` and `updated_at`
columns).
These rows are then upserted into the follower (receiver) database.
This fetch-upsert process loops indefinitely.

In order to bootstrap the synchronization, a database dump is created from the leader database and imported into the
follower database.
When starting `pg_unprivileged_replication` for the first time, a timestamp dating before this dump must be provided.

After each fetch-upsert loop, a record of the replication is inserted into a `unprivileged_replication_sync_records`
table containing the `started_at` and `finished_at` timestamps for the loop.
Before each fetch-upsert loop, the latest record of this table is queried, and the changes dating later than the
`started_at` at fetched.


## Limitations

Some known limitations:

- `pg_unprivileged_replication` does not update sequences, if you wish to fail over to the follower database you must
  manually synchronize sequences.
- The follower database will not be "in recovery mode", and so it may be mistakenly written to by a poorly configured
  client.
- Like PostgreSQL logical replication, `pg_unprivileged_replication` does not synchronize schema changes.
  Furthermore, unlike PostgreSQL logical replication, the service must be restarted in order for new columns to be
  recognized and will certainly crash of columns are dropped.
- The `id` and `updated_at` columns may not provide consistent and accurate semantics depending on how writers to the
  leading database populate them.
- Only some column types are supported.
  It should work with basic integer, float, decimal, text, and binary types, timestamps, booleans, and UUIDs.
  One dimensional arrays are also supported, but `NULL` arrays will be implicitly cast to empty arrays.
  It may be possible to add support for other types with minor modifications to `pgdump/column.go`.
- Best consistency is obtained when `pg_unprivileged_replication` is using `serializable` transactions, which may cause
  some addition transactions to fail if you are using the `serializable` transaction isolation level in your
  writters.
- The follower database may be used as a reader with `REPEATABLE READ` isolation level, however transactions will appear
  to be committed irregularly with a significant delay (in large batches).
  If you have any weird races in your application they will likely appear.

## Configuration

`pg_unprivileged_replication` requires a JSON configuration file containing the leader and follower DSN and a list of
tables to synchronize.

JSON 7.0 schema types are used to describe the configuration, Go zero values of the equivalent type are considered the
default when not otherwise specified.

Top-level document keys:
- `leaderDSN`: `postgres://` URL for the leader database. (required, `string`)
- `followerDSN`: `postgres://` URL for the follower database. (required, `string`)
- `run`: configuration about the service execution. (`object`)
  - `iterationSleepInterval`: The amount of time in seconds to sleep between each fetch-upsert loop.
     This allows you to control CPU load on the leader and follower database as well as the
     `pg_unprivileged_replication` services, especially when the follower is just behind the leader.
     (`number`)
  - `exitOnCompletion`: Whether or not to exit when the databases are fully synchronized. (`boolean`)
- `sync`: global configuration about the synchronization heuristics. (`object`)
  - `clockSynchronizationMarginSeconds`: The amount of time in seconds to subtract for the previous `started_at` sync
    record when querying rows to fetch.
    This is to prevent clock-skew and insert latency between `pg_unprivileged_replication` and writer clients from
    resulting in inconsistent synchronization.
    It is recommended to set this value to _at least_ 5 seconds, however there is no default value. (`number`)
- `tables`: List of tables to synchronize, all other tables not specified will be ignored. (`array` of `object`s)
  - `name`: The table name. (required, `string`)
  - `replicationMode`: The kind of replication logic to apply when fetching rows from the leader database.
    One of `upsert`, `insert`, or `insert-serial`.
    See below for a description of each. (`string`, default: `upsert`)
  - `idColumn`: The column name to use to uniquely identify the column when upserting, should be the `PRIMARY KEY`.
    Also used in the `insert` and `insert-serial` replication modes, is assumed to be autoincrementing.  (`string`,
    default: `id`)
  - `createdAtColumn`: The column name to use when `maxRecordAgeSeconds` is provided to determine when the row was
    created.
    It is ignored in `insert-serial` replication mode.  (`string`, default: `created_at`)
  - `updatedAtColumn`: The column name to use when comparing the last sync `started_at` time for differential column
    update.
    It is ignored in `insert-serial` replication mode. (`string`, default: `updated_at`)
  - `maxRecordAgeSeconds`: The amount of time in seconds since a rows creation to ignore updates to a row for.
    It is designed to heuristically optimizize table scans when an index is present on the `created_at` column, and
    either no updates are expected to a row that is this old or updates are no longer relevant to track.
    It should not be used when your `PRIMARY KEY` is not autoincrementing (`number`)

### Replication modes

There are three replication modes that can be used on tables.

- `upsert`: This is the default and most versatile replication mode, it is also the slowest.
  It compares the update time of a row with the last synchronization `started_at` time, synchronizing all rows which
  were updated later than `started_at` minus `clockSynchronizationMarginSeconds` seconds (if any).
  It attempts to insert the updated row into the follower database, if it conflicts it will instead attempt to overwrite
  the row with the same `PRIMARY KEY`.
  If `maxRecordAgeSeconds` is provided it will ignore all rows created before `started_at` minus `maxRecordAgeSeconds`
  seconds.
  It is _critical_ that `clockSynchronizationMarginSeconds` is sufficiently large for your application for this
  replication mode to work reliably.
- `insert`: This is likely the least useful replication mode.
  It is like `upsert`, except it will not attempt to overwrite rows and will simply fail if a conflict occurs.
  It is designed for cases like `insert-serial` when the `PRIMARY KEY` column cannot be relied upon as being serially
  updated (for example when the primary key is a UUID or multiple statements are executed in a transaction when rows are
  inserted into this table).
- `insert-serial`: This is the fastest but also most limited replication mode.
  It does not attempt to provide any consistency or error checking on updates to rows and relies only on a `PRIMARY KEY`
  column which is monotonically increasing with time for all inserted rows.
  It is useful for tables which are too expensive to use `upsert` or are lacking an updated timestamp, but are only
  inserted and never updated.

### Example configuration:

```json
{
  "leaderDSN": "postgres://user:pass@leaderhost:5432/database",
  "followerDSN": "postgres://user:pass@followerhost:5432/database",
  "run": {
    "iterationSleepInterval": 10.0,
    "exitOnCompletion": true
  },
  "sync": {
    "clockSynchronizationMarginSeconds": 5.0
  },
  "tables": [{
    "name": "normal_table"
  }, {
    "name": "weirder_table",
    "idColumn": "identity",
    "createdAtColumn": "createdAt",
    "updatedAtColumn": "updatedAt",
    "maxRecordAgeSeconds": 3600.0
  }, {
    "name": "without_serial_id_that_is_only_ever_updated_table",
    "replicationMode": "insert",
    "idColumn": "uuid"
  }, {
    "name": "mass_insert_table",
    "replicationMode": "insert-serial"
  }]
}
```

## Help text

```
Usage of pg_unprivileged_replication:
  -config string
    	Path to configuraiton file (ex. config.json)
  -start-at string
    	Manually force last-sync start time (useful when starting from a freshly-restored PGDUMP)
  -verbose
    	Be verbose
```
