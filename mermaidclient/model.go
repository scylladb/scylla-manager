// Copyright (C) 2017 ScyllaDB

package mermaidclient

import (
	"fmt"
	"io"
	"text/template"

	"github.com/scylladb/mermaid/internal/inexlist"
	"github.com/scylladb/mermaid/mermaidclient/internal/models"
	"github.com/scylladb/mermaid/mermaidclient/table"
)

// ErrorResponse is returned in case of an error.
type ErrorResponse = models.ErrorResponse

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
	t := table.New("cluster id", "name")
	for _, c := range cs {
		t.AddRow(c.ID, c.Name)
	}
	if _, err := w.Write([]byte(t.String())); err != nil {
		return err
	}

	return nil
}

// Task is a sched.Task representation.
type Task = models.Task

func makeTaskUpdate(t *Task) *models.TaskUpdate {
	return &models.TaskUpdate{
		Type:       t.Type,
		Enabled:    t.Enabled,
		Name:       t.Name,
		Schedule:   t.Schedule,
		Tags:       t.Tags,
		Properties: t.Properties,
	}
}

// Target is a representing results of dry running repair task.
type Target struct {
	models.RepairTarget
}

const targetTemplate = `{{ if ne .TokenRanges "dcpr" -}}
Token Ranges: {{ .TokenRanges }}
{{ end -}}
{{ if .Host -}}
Host: {{ .Host }}
{{ end -}}
{{ if .WithHosts -}}
With Hosts:
{{ range .WithHosts -}}
  - {{ . }}
{{ end -}}
{{ end -}}
Data Centers:
{{ range .Dc }}  - {{ . }}
{{ end -}}
{{ range .Units }}Keyspace: {{ .Keyspace }}
{{- if .AllTables }}
  (all tables)
{{ else -}}
{{ range .Tables }}
  - {{ . }}
{{ end -}}
{{- end -}}
{{ end }}`

// Render implements Renderer interface.
func (t Target) Render(w io.Writer) error {
	temp := template.Must(template.New("target").Parse(targetTemplate))
	return temp.Execute(w, t)
}

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
	p := table.New("task", "arguments", "next run", "status")
	p.LimitColumnLength(3)
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
		if t.Status == "ERROR" && t.Schedule.NumRetries > 0 {
			s += fmt.Sprintf(" (%d/%d)", t.Failures, t.Schedule.NumRetries+1)
		}
		pr := NewCmdRenderer(&Task{
			ClusterID:  t.ClusterID,
			Type:       t.Type,
			Schedule:   t.Schedule,
			Properties: t.Properties,
		}, RenderTypeArgs).String()
		p.AddRow(id, pr, r, s)
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
		t.AddRow(r.ID, FormatTime(r.StartTime), FormatTime(r.EndTime), FormatDuration(r.StartTime, r.EndTime), s)
	}
	if _, err := w.Write([]byte(t.String())); err != nil {
		return err
	}
	return nil
}

// RepairProgress contains shard progress info.
type RepairProgress struct {
	*models.TaskRunRepairProgress
	Task     *Task
	Detailed bool

	hostFilter     inexlist.InExList
	keyspaceFilter inexlist.InExList
}

// SetHostFilter adds filtering rules used for rendering for host details.
func (rp *RepairProgress) SetHostFilter(filters []string) (err error) {
	rp.hostFilter, err = inexlist.ParseInExList(filters)
	return
}

// SetKeyspaceFilter adds filtering rules used for rendering for keyspace details.
func (rp *RepairProgress) SetKeyspaceFilter(filters []string) (err error) {
	rp.keyspaceFilter, err = inexlist.ParseInExList(filters)
	return
}

// hideUnit returns true if unit data doesn't match host and keyspace filters.
// This includes keyspaces that don't have references to filtered hosts.
func (rp RepairProgress) hideUnit(u *models.RepairProgressUnitsItems0) bool {
	if u == nil {
		return true
	}

	if rp.hostFilter.Size() > 0 {
		match := false
		for _, n := range u.Nodes {
			if rp.hostFilter.FirstMatch(n.Host) != -1 {
				match = true
				break
			}
		}
		if !match {
			return true
		}
	}

	if rp.keyspaceFilter.Size() > 0 {
		if rp.keyspaceFilter.FirstMatch(u.Unit.Keyspace) == -1 {
			return true
		}
	}
	return false
}

func (rp RepairProgress) hideHost(host string) bool {
	if rp.hostFilter.Size() > 0 {
		return rp.hostFilter.FirstMatch(host) == -1
	}
	return false
}

// Render renders *RepairProgress in a tabular format.
func (rp RepairProgress) Render(w io.Writer) error {
	if err := rp.addHeader(w); err != nil {
		return err
	}

	if rp.Progress != nil {
		t := table.New()
		rp.addRepairUnitProgress(t)
		if _, err := io.WriteString(w, t.String()); err != nil {
			return err
		}
	}

	if rp.Progress != nil && rp.Detailed {
		d := table.New()
		addSeparator := false
		for _, u := range rp.Progress.Units {
			if rp.hideUnit(u) {
				continue
			}
			if addSeparator {
				d.AddSeparator()
			}
			d.AddRow(u.Unit.Keyspace, "shard", "progress", "segment_count", "segment_success", "segment_error")
			addSeparator = true
			if len(u.Nodes) > 0 {
				d.AddSeparator()
				rp.addRepairUnitDetailedProgress(d, u)
			}
		}
		if _, err := w.Write([]byte(d.String())); err != nil {
			return err
		}
	}
	return nil
}

var progressTemplate = `{{ if arguments }}Arguments:	{{ arguments }}
{{ end -}}
{{ with .Run }}Status:		{{ .Status }}
{{- if .Cause }}
Cause:		{{ .Cause }}
{{- end }}
{{- if not (isZero .StartTime) }}
Start time:	{{ FormatTime .StartTime }}
{{- end -}}
{{- if not (isZero .EndTime) }}
End time:	{{ FormatTime .EndTime }}
{{- end }}
Duration:	{{ FormatDuration .StartTime .EndTime }}
{{ end -}}
{{ with .Progress }}Progress:	{{ FormatProgress .PercentComplete .PercentFailed }}
{{- if .Ranges }}
Token ranges:	{{ .Ranges }}
{{ end -}}
{{ if .Dcs }}
Datacenters:	{{ range .Dcs }}
  - {{ . }}
{{- end }}
{{ end -}}
{{ else }}Progress:	0%
{{ end }}`

func (rp RepairProgress) addHeader(w io.Writer) error {
	temp := template.Must(template.New("repair_progress").Funcs(template.FuncMap{
		"isZero":         isZero,
		"FormatTime":     FormatTime,
		"FormatDuration": FormatDuration,
		"FormatProgress": FormatProgress,
		"arguments":      rp.arguments,
	}).Parse(progressTemplate))
	return temp.Execute(w, rp)
}

// arguments return task arguments that task was created with.
func (rp RepairProgress) arguments() string {
	return NewCmdRenderer(rp.Task, RenderTypeArgs).String()
}

func (rp RepairProgress) addRepairUnitProgress(t *table.Table) {
	for _, u := range rp.Progress.Units {
		if rp.hideUnit(u) {
			continue
		}
		p := "-"
		if len(u.Nodes) > 0 {
			p = FormatProgress(u.PercentComplete, u.PercentFailed)
		}
		t.AddRow(u.Unit.Keyspace, p)
	}
}

func (rp RepairProgress) addRepairUnitDetailedProgress(t *table.Table, u *RepairUnitProgress) {
	for _, n := range u.Nodes {
		if rp.hideHost(n.Host) {
			continue
		}
		for i, s := range n.Shards {
			t.AddRow(n.Host, i, FormatProgress(s.PercentComplete, s.PercentFailed), s.SegmentCount, s.SegmentSuccess, s.SegmentError)
		}
	}
}

// BackupProgress contains shard progress info.
type BackupProgress struct {
	*models.TaskRunBackupProgress
	Task     *Task
	Detailed bool
	Errors   []string

	hostFilter     inexlist.InExList
	keyspaceFilter inexlist.InExList
}

// SetHostFilter adds filtering rules used for rendering for host details.
func (rp *BackupProgress) SetHostFilter(filters []string) (err error) {
	rp.hostFilter, err = inexlist.ParseInExList(filters)
	return
}

// SetKeyspaceFilter adds filtering rules used for rendering for keyspace details.
func (rp *BackupProgress) SetKeyspaceFilter(filters []string) (err error) {
	rp.keyspaceFilter, err = inexlist.ParseInExList(filters)
	return
}

// AggregateErrors collects all errors from the table progress.
func (rp *BackupProgress) AggregateErrors() {
	for i := range rp.Progress.Hosts {
		for j := range rp.Progress.Hosts[i].Keyspaces {
			for _, t := range rp.Progress.Hosts[i].Keyspaces[j].Tables {
				if t.Error != "" {
					rp.Errors = append(rp.Errors, t.Error)
				}
			}
		}
	}
}

// Render renders *BackupProgress in a tabular format.
func (rp BackupProgress) Render(w io.Writer) error {
	if err := rp.addHeader(w); err != nil {
		return err
	}

	if rp.Progress != nil && rp.Progress.Size > 0 {
		t := table.New()
		rp.addHostProgress(t)
		if _, err := io.WriteString(w, t.String()); err != nil {
			return err
		}
	}

	if rp.Detailed && rp.Progress != nil && rp.Progress.Size > 0 {
		if err := rp.addKeyspaceProgress(w); err != nil {
			return err
		}
	}
	return nil
}

func (rp BackupProgress) addHostProgress(t *table.Table) {
	t.AddRow("host", "progress", "size", "uploaded", "skipped", "failed")
	t.AddSeparator()
	for _, h := range rp.Progress.Hosts {
		if rp.hideHost(h.Host) {
			continue
		}
		p := "-"
		if len(h.Keyspaces) > 0 {
			p = FormatUploadProgress(h.Size, h.Uploaded, h.Skipped)
		}
		t.AddRow(h.Host, p,
			ByteCountBinary(h.Size),
			ByteCountBinary(h.Uploaded),
			ByteCountBinary(h.Skipped),
			ByteCountBinary(h.Failed),
		)
	}
}

func (rp BackupProgress) addKeyspaceProgress(w io.Writer) error {
	for _, h := range rp.Progress.Hosts {
		if rp.hideHost(h.Host) {
			continue
		}
		fmt.Fprintf(w, "Host:		%s\n", h.Host)
		fmt.Fprintf(w, "Started At:	%s\nCompleted At:	%s\n", h.StartedAt, h.CompletedAt)
		t := table.New()
		addSeparator := false
		for _, ks := range h.Keyspaces {
			if rp.hideKeyspace(ks.Keyspace) {
				break
			}
			if addSeparator {
				t.AddSeparator()
			}
			addSeparator = true

			t.AddRow("keyspace", "table", "progress", "size", "uploaded", "skipped", "failed", "started at", "completed at")
			t.AddSeparator()
			rowAdded := false
			for _, tbl := range ks.Tables {
				t.AddRow(
					ks.Keyspace,
					tbl.Table,
					FormatUploadProgress(tbl.Size,
						tbl.Uploaded,
						tbl.Skipped),
					ByteCountBinary(tbl.Size),
					ByteCountBinary(tbl.Uploaded),
					ByteCountBinary(tbl.Skipped),
					ByteCountBinary(tbl.Failed),
					tbl.StartedAt,
					tbl.CompletedAt,
				)
				rowAdded = true
			}
			if !rowAdded {
				// Separate keyspaces with no table rows
				t.AddRow("-", "-", "-", "-", "-", "-", "-", "-", "-")
			}
		}
		if _, err := w.Write([]byte(t.String())); err != nil {
			return err
		}
	}
	return nil
}

func (rp BackupProgress) hideHost(host string) bool {
	if rp.hostFilter.Size() > 0 {
		return rp.hostFilter.FirstMatch(host) == -1
	}
	return false
}

func (rp BackupProgress) hideKeyspace(keyspace string) bool {
	if rp.keyspaceFilter.Size() > 0 {
		return rp.keyspaceFilter.FirstMatch(keyspace) == -1
	}
	return false
}

var backupProgressTemplate = `{{ if arguments }}Arguments:	{{ arguments }}
{{ end -}}
{{ with .Run }}Status:		{{ .Status }}
{{- if .Cause }}
Cause:		{{ .Cause }}
{{- end }}
{{- if not (isZero .StartTime) }}
Start time:	{{ FormatTime .StartTime }}
{{- end -}}
{{- if not (isZero .EndTime) }}
End time:	{{ FormatTime .EndTime }}
{{- end }}
Duration:	{{ FormatDuration .StartTime .EndTime }}
{{ end -}}
{{ with .Progress }}Progress:	{{ FormatUploadProgress .Size .Uploaded .Skipped }}
{{- if .Dcs }}
Datacenters:	{{ range .Dcs }}
  - {{ . }}
{{- end }}
{{ end -}}
{{ else }}Progress:	0%
{{ end }}
{{- if .Errors -}}
Errors:	{{ range .Errors }}
  - {{ . }}
{{- end }}
{{ end -}}`

func (rp BackupProgress) addHeader(w io.Writer) error {
	temp := template.Must(template.New("backup_progress").Funcs(template.FuncMap{
		"isZero":               isZero,
		"FormatTime":           FormatTime,
		"FormatDuration":       FormatDuration,
		"FormatUploadProgress": FormatUploadProgress,
		"arguments":            rp.arguments,
	}).Parse(backupProgressTemplate))
	return temp.Execute(w, rp)
}

// arguments return task arguments that task was created with.
func (rp BackupProgress) arguments() string {
	return NewCmdRenderer(rp.Task, RenderTypeArgs).String()
}

const statusDown = "DOWN"

// ClusterStatus contains cluster status info.
type ClusterStatus models.ClusterStatus

// Render renders ClusterStatus in a tabular format.
func (cs ClusterStatus) Render(w io.Writer) error {
	if len(cs) == 0 {
		return nil
	}

	var (
		dc = cs[0].Dc
		t  = table.New("CQL", "SSL", "REST", "Host")
	)

	for _, s := range cs {
		if s.Dc != dc {
			if _, err := w.Write([]byte("Datacenter: " + dc + "\n" + t.String())); err != nil {
				return err
			}
			dc = s.Dc
			t = table.New("CQL", "SSL", "REST", "Host")
		}
		cqlStatus := statusDown
		if s.CqlStatus != statusDown {
			cqlStatus = fmt.Sprintf("%s (%.0fms)", s.CqlStatus, s.CqlRttMs)
		}
		restStatus := statusDown
		if s.RestStatus != statusDown {
			restStatus = fmt.Sprintf("%s (%.0fms)", s.RestStatus, s.RestRttMs)
		}
		ssl := "OFF"
		if s.Ssl {
			ssl = "ON"
		}
		t.AddRow(cqlStatus, ssl, restStatus, s.Host)
	}
	if _, err := w.Write([]byte("Datacenter: " + dc + "\n" + t.String())); err != nil {
		return err
	}

	return nil
}

// RepairUnitProgress contains unit progress info.
type RepairUnitProgress = models.RepairProgressUnitsItems0
