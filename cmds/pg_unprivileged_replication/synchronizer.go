package main

import (
	"database/sql"
	"fmt"
	"log"
	"time"

	"github.com/tomyl/pg-dump-upsert/pgdump"
	"golang.org/x/net/context"
)

type synchronizer struct {
	leader   *sql.DB
	leaderTx *sql.Tx
	follower *sql.DB
	c        syncConfig
	updated  uint64
	verbose  bool
}

func (s *synchronizer) syncAll(lastSync time.Time, ts []tableConfig) {
	s.updated = 0

	var err error
	ctx := context.Background()
	txOptions := sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true}
	if s.leaderTx, err = s.leader.BeginTx(ctx, &txOptions); err != nil {
		log.Panicf("Failed to create leader transaction for sync: %v\n", err)
	}

	for _, t := range ts {
		tableSyncStart := time.Now()
		origUpdated := s.updated
		log.Printf("Syncing table %s ...\n", t.Name)

		s.sync(lastSync, t)

		log.Printf("done in %s, updated %d rows.\n", time.Since(tableSyncStart).String(), s.updated-origUpdated)
	}
}

func (s *synchronizer) sync(lastSync time.Time, t tableConfig) {
	switch t.ReplicationMode {
	case "", "upsert":
		s.syncUpsert(lastSync, t)
	case "insert":
		s.syncInsert(lastSync, t)
	case "insert-serial":
		s.syncInsertSerial(lastSync, t)
	default:
		log.Panicf("Unknown replication mode %s requested for table %s\n", t.ReplicationMode, t.Name)
	}
}

func (s *synchronizer) syncUpsert(lastSync time.Time, t tableConfig) {
	minId := s.partionByAge(lastSync, t)
	margin := time.Duration(float64(time.Second) * s.c.ClockSynchronizationMarginSeconds)
	updatedAt := lastSync.Add(-1 * margin).Format(time.RFC3339)
	s.dump(t, &pgdump.Options{
		ConflictColumn: t.IdColumn,
		Query: fmt.Sprintf(
			"WHERE %s >= %d AND %s >= TIMESTAMP '%s'",
			t.IdColumn, minId, t.UpdatedAtColumn, updatedAt),
		Verbose: s.verbose,
	})
}

func (s *synchronizer) syncInsert(lastSync time.Time, t tableConfig) {
	minId := s.partionByAge(lastSync, t)
	margin := time.Duration(float64(time.Second) * s.c.ClockSynchronizationMarginSeconds)
	updatedAt := lastSync.Add(-1 * margin).Format(time.RFC3339)

	s.dump(t, &pgdump.Options{
		NoConflict: true,
		Query: fmt.Sprintf(
			`WHERE %s >= %d AND %s >= TIMESTAMP '%s'`,
			t.IdColumn, minId, t.UpdatedAtColumn, updatedAt),
		Verbose: s.verbose,
	})
}

func (s *synchronizer) syncInsertSerial(lastSync time.Time, t tableConfig) {
	row := s.follower.QueryRow(fmt.Sprintf(`SELECT %s FROM %s ORDER BY 1 DESC LIMIT 1`, t.IdColumn, t.Name))
	var lastId, minId int64
	if err := row.Scan(&lastId); err == sql.ErrNoRows {
		minId = 0
	} else if err != nil {
		log.Panicf("Failed to get last ID from follower on table %s: %v\n", t.Name, err)
	} else {
		minId = lastId + 1
	}

	s.dump(t, &pgdump.Options{
		Query:   fmt.Sprintf("WHERE %s >= %d", t.IdColumn, minId),
		Verbose: s.verbose,
	})
}

func (s *synchronizer) execOnFollower(st string) error {
	s.updated += 1
	_, err := s.follower.Exec(st)
	return err
}

func (s *synchronizer) dump(t tableConfig, pgdumpOpts *pgdump.Options) {
	if err := pgdump.Dump(s.execOnFollower, pgdump.NewQuerier(s.leaderTx), t.Name, pgdumpOpts); err != nil {
		log.Panicf("Failed to dump table %s: %v\n", t.Name, err)
	}
}

func (s *synchronizer) partionByAge(lastSync time.Time, t tableConfig) int64 {
	if t.MaxRecordAgeSeconds == 0 {
		return 0
	}

	age := time.Duration(float64(time.Second) * t.MaxRecordAgeSeconds)
	creationTimestamp := lastSync.Add(-1 * age).Format(time.RFC3339)
	st := fmt.Sprintf(
		"SELECT %s FROM %s WHERE %s < TIMESTAMP '%s' ORDER BY 1 DESC LIMIT 1",
		t.IdColumn, t.Name, t.CreatedAtColumn, creationTimestamp)
	row := s.leaderTx.QueryRow(st)

	var id int64
	if err := row.Scan(&id); err == sql.ErrNoRows {
		return 0
	} else if err != nil {
		log.Panicf("Failed to find partition id by age for table %s: %v\n", t.Name, err)
	}

	return id + 1
}
