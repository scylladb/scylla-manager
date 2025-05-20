package main

import (
	"context"
	"database/sql"
	_ "embed"
	"flag"
	"log"

	"github.com/scylladb/scylla-manager/v3/sqlc/queries"
	_ "modernc.org/sqlite"
)

//go:embed schema.sql
var schema string

func main() {
	dataPath := flag.String("data-path", "./db.sqlite", "Path to the data directory")
	action := flag.String("action", "list", "Action to perform")
	flag.Parse()

	ctx := context.Background()

	db, err := sql.Open("sqlite", *dataPath)
	if err != nil {
		log.Fatalf("failed to open database: %v", err)
	}

	if _, err := db.ExecContext(ctx, schema); err != nil {
		log.Fatalf("failed to create schema: %v", err)
	}

	repairQueries := queries.New(db)

	if *action == "list" {
		log.Println("List all repair run progress")
		if err != nil {
			log.Fatalf("failed to get repair progress: %v", err)
		}
		prgs, err := repairQueries.GetRepairRunProgress(ctx, queries.GetRepairRunProgressParams{
			ClusterID: "cluster-id",
			TaskID:    "task-id",
			RunID:     "run-id",
		})
		if err != nil {
			log.Fatalf("failed to get repair progress: %v", err)
		}
		for _, prg := range prgs {
			log.Printf("Repair progress: %v", prg)
		}

		log.Println("List all repair runs")
		runs, err := repairQueries.GetRepairRun(ctx, queries.GetRepairRunParams{
			ClusterID: "cluster-id",
			TaskID:    "task-id",
			ID:        "run-id",
		})
		if err != nil {
			log.Fatalf("failed to get repair runs: %v", err)
		}
		for _, run := range runs {
			log.Printf("Repair run: %v", run)
		}
		log.Println("List all repair run states")
		states, err := repairQueries.GetRepairRunState(ctx, queries.GetRepairRunStateParams{
			ClusterID: "cluster-id",
			TaskID:    "task-id",
			RunID:     "run-id",
		})
		if err != nil {
			log.Fatalf("failed to get repair run states: %v", err)
		}
		for _, state := range states {
			log.Printf("Repair run state: %v", state)
		}

		return
	}
	if *action == "insert" {
		log.Println("Insert repair run progress")
		prg := queries.InsertRepairRunProgressParams{
			ClusterID:         "cluster-id",
			TaskID:            "task-id",
			RunID:             "run-id",
			Host:              "host",
			KeyspaceName:      "keyspace-name",
			TableName:         "table-name",
			CompletedAt:       sql.NullTime{},
			Duration:          0,
			DurationStartedAt: sql.NullTime{},
			Error:             0,
			Size:              0,
			StartedAt:         sql.NullTime{},
			Success:           0,
			TokenRanges:       0,
		}
		if err := repairQueries.InsertRepairRunProgress(ctx, prg); err != nil {
			log.Fatalf("failed to insert repair run progress: %v", err)
		}

		log.Println("Insert repair run state")
		state := queries.InsertRepairRunStateParams{
			ClusterID:     "cluster-id",
			TaskID:        "task-id",
			RunID:         "run-id",
			KeyspaceName:  "keyspace-name",
			TableName:     "table-name",
			SuccessRanges: []byte("[0, 1, 2]"),
		}
		if err := repairQueries.InsertRepairRunState(ctx, state); err != nil {
			log.Fatalf("failed to insert repair run state: %v", err)
		}

		return
	}
	log.Fatalf("unknown action: %s", *action)
}
