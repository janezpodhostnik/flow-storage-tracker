package main

import (
	tracker "flow-storage-tracker"
	"fmt"
	"github.com/axiomzen/envconfig"
)

func main() {
	var config tracker.Config
	err := envconfig.Process("FLOW", &config)
	if err != nil {
		// logger is not initialized yet, so we have to panic
		panic("could not load Config")
	}

	var db tracker.DBConfig
	err = envconfig.Process("FLOW_DB", &db)
	if err != nil {
		// logger is not initialized yet, so we have to panic
		panic("could not load DB Config")
	}

	connectionString := fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=disable", db.User, db.Password, db.Host, db.Port, db.Name)

	tracker.Scan(config.FlowAccessNodeURL, config.BatchSize, connectionString)
}
