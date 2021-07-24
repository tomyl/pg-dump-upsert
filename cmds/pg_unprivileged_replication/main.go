package main

import (
	"database/sql"
	"encoding/json"
	"flag"
	"log"
	"os"
	"os/signal"
	"time"

	"github.com/tomyl/pg-dump-upsert/pgdump"
)

func main() {
	configFilePath := flag.String("config", "", "Path to configuraiton file (ex. config.json)")
	startAt := flag.String("start-at", "",
		"Manually force last-sync start time (useful when starting from a freshly-restored PGDUMP)")
	verbose := flag.Bool("verbose", false, "Be verbose")
	flag.Parse()

	if configFilePath == nil || *configFilePath == "" {
		log.Fatalln("The -config arguement is mandatory, see -help for details.")
	}

	var c config
	if configFile, err := os.OpenFile(*configFilePath, os.O_RDONLY, 0); err != nil {
		log.Panicf("Couldn't open config file: %v\n", err)
	} else {
		err := json.NewDecoder(configFile).Decode(&c)
		configFile.Close()
		if err != nil {
			log.Panicf("Couldn't load config: %v\n", err)
		}
	}

	s := &synchronizer{
		c:       c.Sync,
		verbose: *verbose,
	}

	if leaderDb, err := sql.Open("postgres", c.LeaderDSN); err != nil {
		log.Panicf("Failed to open leader database: %v\n", err)
	} else {
		defer leaderDb.Close()
		s.leader = leaderDb
	}

	var followerQ pgdump.Querier
	if followerDb, err := sql.Open("postgres", c.FollowerDSN); err != nil {
		log.Panicf("Failed to open flollower database: %v\n", err)
	} else {
		defer followerDb.Close()
		s.follower = followerDb
		followerQ = pgdump.NewQuerier(followerDb)
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

	var lastSync *syncRecord
	var err error
	createTableSyncRecords(followerQ)
	if *startAt != "" {
		if t, err := time.Parse(time.RFC3339, *startAt); err != nil {
			log.Panicf("Couldn't parse start time provided by -start-at: %v\n", err)
		} else {
			lastSync = &syncRecord{startedAt: t}
			log.Printf("Manual start time provided, starting at %s\n", lastSync.startedAt.String())
		}
	} else if lastSync, err = lastFinishedSync(followerQ); err == sql.ErrNoRows {
		lastSync = &syncRecord{startedAt: time.Unix(0, 0)}
		log.Printf("Found no completed syncRecord, starting at %s\n", lastSync.startedAt.String())
	} else if err != nil {
		log.Panicf("Failed to query last completed syncRecord: %v\n", err)
	} else {
		log.Printf("Found last sync record (%d), starting at %s\n", *lastSync.id, lastSync.startedAt.String())
	}

	interrupts := make(chan os.Signal, 1)
	signal.Notify(interrupts, os.Interrupt)

	for {
		currentSync := &syncRecord{
			startedAt: time.Now(),
		}
		if err := currentSync.create(followerQ); err != nil {
			log.Panicf("Failed to create new syncRecord before loop: %v\n", err)
		}

		log.Printf("Starting sync of all tables at %s.\n", currentSync.startedAt.String())

		s.syncAll(lastSync.startedAt, c.Tables)

		currentSync.finish()
		if err := currentSync.save(followerQ); err != nil {
			log.Panicf("Failed to save syncRecord after loop: %v\n", err)
		}
		lastSync = currentSync

		log.Printf(
			"Completed sync of all tables in %s, updated %d rows across all tables.\n",
			currentSync.duration().String(), s.updated)

		if s.updated == 0 && c.Run.ExitOnCompletion {
			log.Println("Exiting because the follower has caught up with leader.")
			break
		}
		select {
		case <-interrupts:
			log.Println("Exiting because interrupt recieved.")
			return
		case <-time.After(time.Duration(float64(time.Second) * c.Run.IterationSleepInterval)):
			// continue execution.
		}
	}
}
