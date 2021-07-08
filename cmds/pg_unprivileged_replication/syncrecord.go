package main

import (
	"fmt"
	"log"
	"time"

	"github.com/lib/pq"
	"github.com/tomyl/pg-dump-upsert/pgdump"
)

type syncRecord struct {
	id         *int64
	startedAt  time.Time
	finishedAt pq.NullTime
}

func createTableSyncRecords(q pgdump.Querier) {
	if _, err := q.Exec(`
		CREATE TABLE IF NOT EXISTS unprivileged_replication_sync_records (
			id bigserial PRIMARY KEY,
			started_at timestamp without time zone NOT NULL,
			finished_at timestamp without time zone
		);
	`); err != nil {
		log.Panicf("Unable to create %s table (syncRecord): %v\n", "unprivileged_replication_sync_records", err)
	}
}

func lastFinishedSync(q pgdump.Querier) (*syncRecord, error) {
	r := &syncRecord{
		id: new(int64),
	}
	row := q.QueryRow(`
		SELECT id, started_at, finished_at
		FROM unprivileged_replication_sync_records
		WHERE finished_at IS NOT NULL
		ORDER BY started_at DESC
		LIMIT 1;
	`)
	if err := row.Scan(r.id, &r.startedAt, &r.finishedAt); err != nil {
		return nil, err
	}
	return r, nil
}

func (r *syncRecord) create(q pgdump.Querier) error {
	if r.id != nil {
		log.Panicln("create called on already-created syncRecord")
	}

	row := q.QueryRow(`
		INSERT INTO unprivileged_replication_sync_records (started_at, finished_at)
		VALUES ($1, $2)
		RETURNING id;
	`, r.startedAt, r.finishedAt)
	r.id = new(int64)
	if err := row.Scan(r.id); err != nil {
		return err
	}
	return nil
}

func (r syncRecord) save(q pgdump.Querier) error {
	if r.id == nil {
		log.Panicln("save called on uncreated syncRecord")
	}

	if result, err := q.Exec(`
		UPDATE unprivileged_replication_sync_records
		SET started_at = $1, finished_at = $2
		WHERE id = $3;
	`, r.startedAt, r.finishedAt, *r.id); err != nil {
		return err
	} else if affected, err := result.RowsAffected(); err != nil {
		return err
	} else if affected == 0 {
		return fmt.Errorf("couldn't find syncRecord with id %d", r.id)
	}
	return nil
}

func (r *syncRecord) finish() {
	if r.finishedAt.Valid {
		log.Panicln("finish called on already finished syncRecord")
	}

	r.finishedAt = pq.NullTime{
		Time:  time.Now(),
		Valid: true,
	}
}

func (r syncRecord) duration() time.Duration {
	if !r.finishedAt.Valid {
		return 0
	}

	return r.finishedAt.Time.Sub(r.startedAt)
}
