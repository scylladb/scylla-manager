// Copyright (C) 2017 ScyllaDB

package mermaidclient

import (
	"fmt"
	"io"
	"sort"

	"github.com/scylladb/mermaid/mermaidclient/internal/models"
	"github.com/scylladb/mermaid/mermaidclient/table"
)

// TableRenderer is the interface that components need to implement
// if they can render themselves as tables.
type TableRenderer interface {
	Render(io.Writer) error
}

// Cluster is cluster.Cluster representation.
type Cluster = models.Cluster

// ClusterSlice is []*cluster.Cluster representation.
type ClusterSlice []*models.Cluster

// Render renders ClusterSlice in a tabular format.
func (cs ClusterSlice) Render(w io.Writer) error {
	t := table.New("cluster id", "name", "host", "ssh user")
	for _, c := range cs {
		t.AddRow(c.ID, c.Name, c.Host, c.SSHUser)
	}
	if _, err := w.Write([]byte(t.String())); err != nil {
		return err
	}

	return nil
}

// Task is a sched.Task representation.
type Task = models.Task

// ExtendedTask is a representation of sched.Task with additional fields from sched.Run.
type ExtendedTask = models.ExtendedTask

// ExtendedTaskSlice is a representation of a slice of sched.Task with additional fields from sched.Run.
type ExtendedTaskSlice = []*models.ExtendedTask

// ExtendedTasks is a representation of []*sched.Task with additional fields from sched.Run.
type ExtendedTasks struct {
	ExtendedTaskSlice
	All bool
}

// Render renders ExtendedTasks in a tabular format.
func (et ExtendedTasks) Render(w io.Writer) error {
	p := table.New("task", "next run", "ret.", "properties", "status")
	for _, t := range et.ExtendedTaskSlice {
		id := fmt.Sprint(t.Type, "/", t.ID)
		if et.All && !t.Enabled {
			id = "*" + id
		}
		r := FormatTime(t.NextActivation)
		if t.Schedule.Interval != "" {
			r += fmt.Sprint(" (+", t.Schedule.Interval, ")")
		}
		s := t.Status
		if t.Cause != "" {
			s += " " + t.Cause
		}
		p.AddRow(id, r, t.Schedule.NumRetries, dumpMap(t.Properties.(map[string]interface{})), s)
	}
	fmt.Fprint(w, p)
	return nil
}

// Schedule is a sched.Schedule representation.
type Schedule = models.Schedule

// TaskRun is a sched.TaskRun representation.
type TaskRun = models.TaskRun

// TaskRunSlice is a []*sched.TaskRun representation.
type TaskRunSlice []*TaskRun

// Render renders TaskRunSlice in a tabular format.
func (tr TaskRunSlice) Render(w io.Writer) error {
	t := table.New("id", "start time", "end time", "duration", "status")
	for _, r := range tr {
		s := r.Status
		if r.Cause != "" {
			s += " " + r.Cause
		}
		t.AddRow(r.ID, FormatTime(r.StartTime), FormatTime(r.EndTime), FormatDuration(r.StartTime, r.EndTime), s)
	}
	if _, err := w.Write([]byte(t.String())); err != nil {
		return err
	}
	return nil
}

// RepairProgress contains shard progress info.
type RepairProgress struct {
	*models.RepairProgress
	Run      *TaskRun
	Detailed bool
}

// Render renders *RepairProgress in a tabular format.
func (rp RepairProgress) Render(w io.Writer) error {
	t := table.New()
	addProgressHeader(t, rp.Run)
	addRepairProgressHeader(t, rp)
	t.AddSeparator()
	addRepairUnitProgress(t, rp)
	if _, err := w.Write([]byte(t.String())); err != nil {
		return err
	}

	if rp.Detailed {
		d := table.New()
		for i, u := range rp.Units {
			if i > 0 {
				d.AddSeparator()
			}
			d.AddRow(u.Unit.Keyspace, "shard", "progress", "segment_count", "segment_success", "segment_error")
			if len(u.Nodes) > 0 {
				d.AddSeparator()
				addRepairUnitDetailedProgress(d, u)
			}
		}
		if _, err := w.Write([]byte(d.String())); err != nil {
			return err
		}
	}
	return nil
}

func addRepairProgressHeader(t *table.Table, prog RepairProgress) {
	t.AddRow("Progress", FormatPercent(prog.PercentComplete))
	if len(prog.Dcs) > 0 {
		t.AddRow("Datacenters", prog.Dcs)
	}
	if prog.Ranges != "" {
		t.AddRow("Token ranges", prog.Ranges)
	}
}

func addProgressHeader(t *table.Table, run *TaskRun) {
	t.AddRow("Status", run.Status)
	if run.Cause != "" {
		t.AddRow("Cause", run.Cause)
	}
	t.AddRow("Start time", FormatTime(run.StartTime))
	if !isZero(run.EndTime) {
		t.AddRow("End time", FormatTime(run.EndTime))
	}
	t.AddRow("Duration", FormatDuration(run.StartTime, run.EndTime))
}

func addRepairUnitProgress(t *table.Table, prog RepairProgress) {
	for _, u := range prog.Units {
		var se int64
		for _, n := range u.Nodes {
			for _, s := range n.Shards {
				se += s.SegmentError
			}
		}
		p := FormatPercent(u.PercentComplete)
		if se > 0 {
			p += " (has errors)"
		}
		t.AddRow(u.Unit.Keyspace, p)
	}
}

func addRepairUnitDetailedProgress(t *table.Table, u *RepairUnitProgress) {
	for _, n := range u.Nodes {
		for i, s := range n.Shards {
			t.AddRow(n.Host, i, fmt.Sprintf("%d%%", s.PercentComplete), s.SegmentCount, s.SegmentSuccess, s.SegmentError)
		}
	}
}

// ClusterStatus contains cluster status info.
type ClusterStatus models.ClusterStatus

// Render renders ClusterStatus in a tabular format.
func (cs ClusterStatus) Render(w io.Writer) error {

	sort.Slice(cs, func(i, j int) bool {
		return cs[i].Host < cs[j].Host
	})

	t := table.New()

	t.AddHeaders("Host", "Status", "RTT (ms)")
	for _, s := range cs {
		t.AddRow(s.Host, s.CqlStatus, s.CqlRttMs)
	}

	if _, err := w.Write([]byte(t.String())); err != nil {
		return err
	}

	return nil
}

// RepairUnitProgress contains unit progress info.
type RepairUnitProgress = models.RepairProgressUnitsItems0
