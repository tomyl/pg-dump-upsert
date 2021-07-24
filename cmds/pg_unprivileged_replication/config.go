package main

type runConfig struct {
	IterationSleepInterval float64 `json:"iterationSleepInterval"`
	ExitOnCompletion       bool    `json:"exitOnCompletion"`
}

type syncConfig struct {
	ClockSynchronizationMarginSeconds float64 `json:"clockSynchronizationMarginSeconds"`
}

type tableConfig struct {
	Name                string  `json:"name"`
	ReplicationMode     string  `json:"replicationMode"`
	IdColumn            string  `json:"idColumn"`
	CreatedAtColumn     string  `json:"createdAtColumn"`
	UpdatedAtColumn     string  `json:"updatedAtColumn"`
	MaxRecordAgeSeconds float64 `json:"maxRecordAgeSeconds"`
}

type config struct {
	LeaderDSN   string `json:"leaderDSN"`
	FollowerDSN string `json:"followerDSN"`

	Run    runConfig     `json:"run"`
	Sync   syncConfig    `json:"sync"`
	Tables []tableConfig `json:"tables"`
}
