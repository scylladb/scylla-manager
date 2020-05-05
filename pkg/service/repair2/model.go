// Copyright (C) 2017 ScyllaDB

package repair

import (
	"time"

	"github.com/scylladb/mermaid/pkg/util/inexlist/ksfilter"
	"github.com/scylladb/mermaid/pkg/util/uuid"
)

// Unit represents keyspace and its tables.
type Unit = ksfilter.Unit

// Target specifies what shall be repaired.
type Target struct {
	Units     []Unit   `json:"units"`
	DC        []string `json:"dc"`
	FailFast  bool     `json:"fail_fast"`
	Continue  bool     `json:"continue"`
	Intensity int      `json:"intensity"`
}

// taskProperties is the main data structure of the runner.Properties blob.
type taskProperties struct {
	Keyspace  []string `json:"keyspace"`
	DC        []string `json:"dc"`
	FailFast  bool     `json:"fail_fast"`
	Continue  bool     `json:"continue"`
	Intensity int      `json:"intensity"`
}

func defaultTaskProperties() *taskProperties {
	return &taskProperties{
		Continue: true,
	}
}

// Run tracks repair, shares ID with scheduler.Run that initiated it.
type Run struct {
	ClusterID uuid.UUID
	TaskID    uuid.UUID
	ID        uuid.UUID

	PrevID    uuid.UUID
	StartTime time.Time

	clusterName string
}

// Progress specifies repair progress of a run.
type Progress struct {
}
