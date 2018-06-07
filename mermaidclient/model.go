// Copyright (C) 2017 ScyllaDB

package mermaidclient

import (
	"github.com/scylladb/mermaid/mermaidclient/internal/models"
)

// Cluster is cluster.Cluster representation.
type Cluster = models.Cluster

// Task is a sched.Task representation.
type Task = models.Task

// ExtendedTask is a representation of sched.Task with additional fields from sched.Run.
type ExtendedTask = models.ExtendedTask

// Schedule is a sched.Schedule representation.
type Schedule = models.Schedule

// TaskRun is a sched.TaskRun representation.
type TaskRun = models.TaskRun

// RepairProgress contains shard progress info.
type RepairProgress = models.RepairProgress
