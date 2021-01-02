package main

import (
	"database/sql"
	"encoding/json"
	"flag"
	"io/ioutil"
	"log"
	"os"
	"os/signal"
	"strings"
	"time"
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

	s := &synchronizer{
		c:       c.Sync,
		verbose: *verbose,
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

	var leaderDb *sql.DB
	if leaderDb, err = sql.Open("postgres", c.LeaderDSN); err != nil {
		log.Panicf("Failed to open leader database: %v\n", err)
	}
	s.leader = leaderDb
	defer leaderDb.Close()

	var followerDb *sql.DB
	if followerDb, err = sql.Open("postgres", c.FollowerDSN); err != nil {
		log.Panicf("Failed to open flollower database: %v\n", err)
	}
	s.follower = followerDb
	defer followerDb.Close()

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

	interrupts := make(chan os.Signal, 1)
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

		log.Printf("Completed sync of all tables in %s, updated %d rows across all tables.\n", time.Since(syncStart).String(), s.updated)
		if s.updated == 0 && c.Run.ExitOnCompletion {
			log.Println("Exiting because the follower has caught up with leader.")
			break
		}
		select {
		case <-interrupts:
			log.Println("Exiting because interrupt recieved.")
			break
		case <-time.After(time.Duration(float64(time.Second) * c.Run.IterationSleepInterval)):
			// continue execution.
		}
	}
}
