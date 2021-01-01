package main

import (
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"os/signal"
	"strings"
	"sync/atomic"
	"time"

	"github.com/tomyl/pg-dump-upsert/pgdump"
)

func main() {
	configFilePath := flag.String("config", "", "Path to configuraiton file (ex. config.json)")
	verbose := flag.Bool("verbose", false, "Be verbose")
	flag.Parse()

	if configFilePath == nil || *configFilePath == "" {
		log.Fatalln("The -config arguement is mandatory, see -help for details.")
	}

	var c config
	if configFile, err := os.OpenFile(*configFilePath, os.O_RDONLY, 0); err != nil {
		log.Panicf("Couldn't open config file: %v\n", err)
	} else if err := json.NewDecoder(configFile).Decode(&c); err != nil {
		log.Panicf("Couldn't load config: %v\n", err)
	}

	s := synchronizer{
		c:       c.Sync,
		verbose: *verbose,
		updated: new(uint64),
	}

	timestampFile, err := os.OpenFile(c.TimestampFilePath, os.O_RDWR, 0)
	if err != nil {
		log.Panicf("Couldn't open timestamp file: %v\n", err)
	}
	defer timestampFile.Close()

	var lastSync time.Time
	if d, err := ioutil.ReadAll(timestampFile); err != nil {
		log.Panicf("Couldn't load timestamp file: %v\n", err)
	} else if lastSync, err = time.Parse(time.RFC3339, strings.TrimSpace(string(d))); err != nil {
		log.Panicf("Couldn't parse timestamp file: %v\n", err)
	}

	if db, err := sql.Open("postgres", c.LeaderDSN); err != nil {
		log.Panicf("Failed to open leader database: %v\n", err)
	} else {
		s.leader = db
	}
	if db, err := sql.Open("postgres", c.FollowerDSN); err != nil {
		log.Panicf("Failed to open flollower database: %v\n", err)
	} else {
		s.follower = db
	}

	for i := range c.Tables {
		t := &c.Tables[i]
		if t.IdColumn == "" {
			t.IdColumn = "id"
		}
		if t.CreatedAtColumn == "" {
			t.CreatedAtColumn = "created_at"
		}
		if t.UpdatedAtColumn == "" {
			t.UpdatedAtColumn = "updated_at"
		}
		if t.MaxRecordAgeSeconds < 0.0 {
			log.Fatalf("tables[].maxRecordAgeSeconds must be equal to or greater than zero")
		}
	}

	shutdownSignalRecvd := int32(0)
	interrupts := make(chan os.Signal)
	go func() {
		for range interrupts {
			log.Println("Recieved SIGINT, quiting when next sync is done.")
			atomic.StoreInt32(&shutdownSignalRecvd, 1)
		}
	}()
	signal.Notify(interrupts, os.Interrupt)

	for {
		syncStart := time.Now()
		log.Printf("Starting sync of all tables at %s.\n", syncStart.String())

		s.syncAll(lastSync, c.Tables)

		lastSync = syncStart
		if _, err := timestampFile.Seek(0, 0); err != nil {
			log.Panicf("Failed to seek on timestamp file: %v\n", err)
		} else if err := timestampFile.Truncate(0); err != nil {
			log.Panicf("Failed to seek truncate timestamp file: %v\n", err)
		} else if _, err := timestampFile.WriteString(lastSync.Format(time.RFC3339) + "\n"); err != nil {
			log.Panicf("Failed to write new timestamp to timestamp file: %v\n", err)
		}

		if 0 != atomic.LoadInt32(&shutdownSignalRecvd) {
			log.Println("Exiting because shutdown signal recieved")
			break
		} else if *s.updated == 0 && c.Run.ExitOnCompletion {
			log.Println("Exiting because follower has caught up with leader")
			break
		}

		time.Sleep(time.Duration(float64(time.Second) * c.Run.IterationSleepInterval))
	}
}

type synchronizer struct {
	leader   *sql.DB
	follower *sql.DB
	c        syncConfig
	updated  *uint64
	verbose  bool
}

func (s synchronizer) syncAll(lastSync time.Time, ts []tableConfig) {
	*s.updated = 0
	for _, t := range ts {
		tableSyncStart := time.Now()
		origUpdated := *s.updated
		log.Printf("Syncing table %s ...\n", t.Name)

		s.sync(lastSync, t)

		log.Printf("done in %s, updated %d rows.\n", time.Now().Local().Sub(tableSyncStart).String(), *s.updated-origUpdated)
	}
}

func (s synchronizer) sync(lastSync time.Time, t tableConfig) {
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

func (s synchronizer) syncUpsert(lastSync time.Time, t tableConfig) {
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

func (s synchronizer) syncInsert(lastSync time.Time, t tableConfig) {
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

func (s synchronizer) syncInsertSerial(lastSync time.Time, t tableConfig) {
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

func (s synchronizer) execOnFollower(st string) error {
	*s.updated += 1
	_, err := s.follower.Exec(st)
	return err
}

func (s synchronizer) dump(t tableConfig, pgdumpOpts *pgdump.Options) {
	if err := pgdump.Dump(s.execOnFollower, s.leader, t.Name, pgdumpOpts); err != nil {
		log.Panicf("Failed to dump table %s: %v\n", t.Name, err)
	}
}

func (s synchronizer) partionByAge(lastSync time.Time, t tableConfig) int64 {
	if t.MaxRecordAgeSeconds == 0 {
		return 0
	}

	age := time.Duration(float64(time.Second) * t.MaxRecordAgeSeconds)
	creationTimestamp := lastSync.Add(-1 * age).Format(time.RFC3339)
	st := fmt.Sprintf(
		"SELECT %s FROM %s WHERE %s < TIMESTAMP '%s' ORDER BY 1 DESC LIMIT 1",
		t.IdColumn, t.Name, t.CreatedAtColumn, creationTimestamp)
	row := s.leader.QueryRow(st)

	var id int64
	if err := row.Scan(&id); err == sql.ErrNoRows {
		return 0
	} else if err != nil {
		log.Panicf("Failed to find partition id by age for table %s: %v\n", t.Name, err)
	}

	return id + 1
}
