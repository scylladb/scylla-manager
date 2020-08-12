// Copyright (C) 2017 ScyllaDB

package mermaidclient

import (
	"fmt"
	"io"
	"text/template"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/scylladb/mermaid/pkg/mermaidclient/internal/models"
	"github.com/scylladb/mermaid/pkg/mermaidclient/table"
	"github.com/scylladb/mermaid/pkg/util/inexlist"
	"github.com/scylladb/mermaid/pkg/util/version"
	"github.com/scylladb/termtables"
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
	t := table.New("ID", "Name")
	for _, c := range cs {
		t.AddRow(c.ID, c.Name)
	}
	if _, err := w.Write([]byte(t.String())); err != nil {
		return err
	}

	return nil
}

// ClusterStatus contains cluster status info.
type ClusterStatus models.ClusterStatus

func (cs ClusterStatus) tableHeaders() []interface{} {
	var (
		headers = []interface{}{"Address", "Uptime", "CPUs", "Memory", "Scylla", "Agent", "Host ID"}
	)

	apis := []interface{}{"CQL", "REST"}

	if cs.hasAnyAlternator() {
		apis = append([]interface{}{"Alternator"}, apis...)
	}

	return append([]interface{}{""}, append(apis, headers...)...)
}

func (cs ClusterStatus) hasAnyAlternator() bool {
	for _, s := range cs {
		if s.AlternatorStatus != "" {
			return true
		}
	}
	return false
}

func (cs ClusterStatus) addRow(t *table.Table, rows ...interface{}) {
	unpacked := make([]interface{}, 0, len(rows))
	for _, r := range rows {
		switch v := r.(type) {
		case []interface{}:
			unpacked = append(unpacked, v...)
		default:
			unpacked = append(unpacked, v)
		}
	}
	headers := cs.tableHeaders()
	t.AddRow(unpacked[:len(headers)]...)
}

// Render renders ClusterStatus in a tabular format.
func (cs ClusterStatus) Render(w io.Writer) error {
	const (
		statusUP         = "UP"
		progressBarWidth = 10
	)

	if len(cs) == 0 {
		return nil
	}

	var (
		dc = cs[0].Dc
		t  = table.New(cs.tableHeaders()...)
	)

	for _, s := range cs {
		if s.Dc != dc {
			if _, err := w.Write([]byte("Datacenter: " + dc + "\n" + t.String())); err != nil {
				return err
			}
			dc = s.Dc
			t = table.New(cs.tableHeaders()...)
		}

		var (
			apiStatuses   []interface{}
			cpus          = "-"
			mem           = "-"
			scyllaVersion = "-"
			agentVersion  = "-"
			uptime        = "-"
		)

		if s.AlternatorStatus != "" {
			status := s.AlternatorStatus
			if s.Ssl {
				status += " SSL"
			}
			apiStatuses = append(apiStatuses, fmt.Sprintf("%s (%.0fms)", status, s.AlternatorRttMs))
		} else if cs.hasAnyAlternator() {
			apiStatuses = append(apiStatuses, "-")
		}

		if s.CqlStatus != "" {
			status := s.CqlStatus
			if s.Ssl {
				status += " SSL"
			}
			apiStatuses = append(apiStatuses, fmt.Sprintf("%s (%.0fms)", status, s.CqlRttMs))
		} else {
			apiStatuses = append(apiStatuses, "-")
		}

		if s.RestStatus != "" {
			apiStatuses = append(apiStatuses, fmt.Sprintf("%s (%.0fms)", s.RestStatus, s.RestRttMs))
		} else {
			apiStatuses = append(apiStatuses, "-")
		}

		if s.RestStatus == statusUP {
			cpus = fmt.Sprintf("%d", s.CPUCount)
			mem = ByteCountBinary(s.TotalRAM)
			scyllaVersion = version.Short(s.ScyllaVersion)
			agentVersion = version.Short(s.AgentVersion)
			uptime = (time.Duration(s.Uptime) * time.Second).String()
		}

		cs.addRow(t, s.Status, apiStatuses, s.Host, uptime, cpus, mem, scyllaVersion, agentVersion, s.HostID)
	}

	if _, err := w.Write([]byte("Datacenter: " + dc + "\n" + t.String())); err != nil {
		return err
	}

	return nil
}

// Task is a scheduler.Task representation.
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

// RepairTarget is a representing results of dry running repair task.
type RepairTarget struct {
	models.RepairTarget
	ShowTables int
}

const repairTargetTemplate = `{{ if ne .TokenRanges "dcpr" -}}
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
{{ end }}
Keyspaces:
{{- range .Units }}
  - {{ .Keyspace }} {{ FormatTables .Tables .AllTables -}}
{{ end }}

`

// Render implements Renderer interface.
func (t RepairTarget) Render(w io.Writer) error {
	temp := template.Must(template.New("target").Funcs(template.FuncMap{
		"FormatTables": func(tables []string, all bool) string {
			return FormatTables(t.ShowTables, tables, all)
		},
	}).Parse(repairTargetTemplate))
	return temp.Execute(w, t)
}

// BackupTarget is a representing results of dry running backup task.
type BackupTarget struct {
	models.BackupTarget
	ShowTables int
}

const backupTargetTemplate = `Data Centers:
{{ range .Dc }}  - {{ . }}
{{ end }}
Keyspaces:
{{- range .Units }}
  - {{ .Keyspace }} {{ FormatTables .Tables .AllTables }}
{{- end }}

Disk size: ~{{ ByteCountBinary .Size }}

Locations:
{{- range .Location }}
  - {{ . }}
{{- end }}

Bandwidth Limits:
{{- if .RateLimit -}}
{{ range .RateLimit }}
  - {{ . }} MiB/s
{{- end }}
{{- else }}
  - Unlimited
{{- end }}

Snapshot Parallel Limits:
{{- if .SnapshotParallel -}}
{{- range .SnapshotParallel }}
  - {{ . }}
{{- end }}
{{- else }}
  - All hosts in parallel
{{- end }}

Upload Parallel Limits:
{{- if .UploadParallel -}}
{{- range .UploadParallel }}
  - {{ . }}
{{- end }}
{{- else }}
  - All hosts in parallel
{{- end }}

Retention: Last {{ .Retention }} backups

`

// Render implements Renderer interface.
func (t BackupTarget) Render(w io.Writer) error {
	temp := template.Must(template.New("target").Funcs(template.FuncMap{
		"ByteCountBinary": ByteCountBinary,
		"FormatTables": func(tables []string, all bool) string {
			return FormatTables(t.ShowTables, tables, all)
		},
	}).Parse(backupTargetTemplate))
	return temp.Execute(w, t)
}

// ExtendedTask is a representation of scheduler.Task with additional fields
// from scheduler.Run.
type ExtendedTask = models.ExtendedTask

// ExtendedTaskSlice is a representation of a slice of scheduler.Task with
// additional fields from scheduler.Run.
type ExtendedTaskSlice = []*models.ExtendedTask

// ExtendedTasks is a representation of []*scheduler.Task with additional
// fields from scheduler.Run.
type ExtendedTasks struct {
	ExtendedTaskSlice
	All bool
}

// Render renders ExtendedTasks in a tabular format.
func (et ExtendedTasks) Render(w io.Writer) error {
	p := table.New("Task", "Arguments", "Next run", "Status")
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

// Schedule is a scheduler.Schedule representation.
type Schedule = models.Schedule

// TaskRun is a scheduler.TaskRun representation.
type TaskRun = models.TaskRun

// TaskRunSlice is a []*scheduler.TaskRun representation.
type TaskRunSlice []*TaskRun

// Render renders TaskRunSlice in a tabular format.
func (tr TaskRunSlice) Render(w io.Writer) error {
	t := table.New("ID", "Start time", "End time", "Duration", "Status")
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
	Task       *Task
	Detailed   bool
	ShowTables int

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

// hideKeyspace returns true if provided keyspace should be hidden.
func (rp RepairProgress) hideKeyspace(keyspace string) bool {
	if rp.keyspaceFilter.Size() > 0 {
		if rp.keyspaceFilter.FirstMatch(keyspace) == -1 {
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

	if rp.Progress == nil {
		return nil
	}

	t := table.New()
	rp.addRepairTableProgress(t)
	t.SetColumnAlignment(termtables.AlignRight, 1)
	if _, err := io.WriteString(w, t.String()); err != nil {
		return err
	}

	if rp.Detailed {
		for _, h := range rp.Progress.Hosts {
			if rp.hideHost(h.Host) {
				continue
			}
			fmt.Fprintf(w, "\nHost: %s\n", h.Host)
			if h.Intensity != 0 {
				fmt.Fprintf(w, "Intensity: %s\n", FormatIntensity(h.Intensity))
			}
			d := table.New()
			d.AddRow("Keyspace", "Table", "Progress", "Token Ranges", "Success", "Error", "Started at", "Completed at", "Duration")
			d.AddSeparator()
			ks := ""
			for _, t := range h.Tables {
				if ks == "" {
					ks = t.Keyspace
				} else if ks != t.Keyspace {
					ks = t.Keyspace
					d.AddSeparator()
				}
				rp.addRepairTableDetailedProgress(d, t)
			}
			d.SetColumnAlignment(termtables.AlignRight, 2, 3, 4, 5, 6, 7, 8)
			if _, err := w.Write([]byte(d.String())); err != nil {
				return err
			}
		}
	}
	return nil
}

var repairProgressTemplate = `{{ if arguments }}Arguments:	{{ arguments }}
{{ end -}}
{{ with .Run }}Status:		{{ .Status }}
{{- if .Cause }}
Cause:		{{ FormatError .Cause }}

{{- end }}
{{- if not (isZero .StartTime) }}
Start time:	{{ FormatTime .StartTime }}
{{- end -}}
{{- if not (isZero .EndTime) }}
End time:	{{ FormatTime .EndTime }}
{{- end }}
Duration:	{{ FormatDuration .StartTime .EndTime }} 
{{- end }}
{{- if ne intensity 0 }}
Intensity:	{{ intensity }} 
{{- end }}
{{ with .Progress }}Progress:	{{ FormatRepairProgress .TokenRanges .Success .Error }}
{{ if .Dcs }}Datacenters:	{{ range .Dcs }}
  - {{ . }}
{{- end }}
{{ end -}}
{{ else }}Progress:	-
{{ end }}
`

func (rp RepairProgress) addHeader(w io.Writer) error {
	temp := template.Must(template.New("repair_progress").Funcs(template.FuncMap{
		"isZero":               isZero,
		"FormatTime":           FormatTime,
		"FormatDuration":       FormatDuration,
		"FormatError":          FormatError,
		"FormatRepairProgress": FormatRepairProgress,
		"arguments":            rp.arguments,
		"intensity":            rp.globalIntensity,
	}).Parse(repairProgressTemplate))
	return temp.Execute(w, rp)
}

// globalIntensity return non zero intensity if all hosts uses the same intensity.
func (rp RepairProgress) globalIntensity() int {
	if rp.Progress == nil || len(rp.Progress.Hosts) == 0 {
		return 0
	}

	intensity := rp.Progress.Hosts[0].Intensity
	for _, h := range rp.Progress.Hosts {
		if h.Intensity != intensity {
			return 0
		}
	}

	return int(intensity)
}

// arguments return task arguments that task was created with.
func (rp RepairProgress) arguments() string {
	return NewCmdRenderer(rp.Task, RenderTypeArgs).String()
}

func (rp RepairProgress) addRepairTableProgress(d *table.Table) {
	if len(rp.Progress.Tables) > 0 {
		d.AddRow("Keyspace", "Table", "Progress", "Duration")
		d.AddSeparator()
	}

	ks := ""
	for _, t := range rp.Progress.Tables {
		if rp.hideKeyspace(t.Keyspace) {
			continue
		}
		if ks == "" {
			ks = t.Keyspace
		} else if ks != t.Keyspace {
			ks = t.Keyspace
			d.AddSeparator()
		}
		p := "-"
		if t.TokenRanges > 0 {
			p = FormatRepairProgress(t.TokenRanges, t.Success, t.Error)
		}
		startedAt := strfmt.DateTime{}
		completedAt := strfmt.DateTime{}
		if t.StartedAt != nil {
			startedAt = *t.StartedAt
			if t.CompletedAt != nil {
				completedAt = *t.CompletedAt
			} else {
				completedAt = rp.Run.EndTime
			}
		}

		d.AddRow(t.Keyspace, t.Table, p, FormatDuration(startedAt, completedAt))
	}
}

func (rp RepairProgress) addRepairTableDetailedProgress(d *table.Table, t *models.TableRepairProgress) {
	if rp.hideKeyspace(t.Keyspace) {
		return
	}
	startedAt := strfmt.DateTime{}
	completedAt := strfmt.DateTime{}
	if t.StartedAt != nil {
		startedAt = *t.StartedAt
		if t.CompletedAt != nil {
			completedAt = *t.CompletedAt
		} else {
			completedAt = rp.Run.EndTime
		}
	}
	d.AddRow(t.Keyspace,
		t.Table,
		FormatRepairProgress(t.TokenRanges, t.Success, t.Error),
		t.TokenRanges,
		t.Success,
		t.Error,
		FormatTime(startedAt),
		FormatTime(completedAt),
		FormatDuration(startedAt, completedAt),
	)
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
func (bp *BackupProgress) SetHostFilter(filters []string) (err error) {
	bp.hostFilter, err = inexlist.ParseInExList(filters)
	return
}

// SetKeyspaceFilter adds filtering rules used for rendering for keyspace details.
func (bp *BackupProgress) SetKeyspaceFilter(filters []string) (err error) {
	bp.keyspaceFilter, err = inexlist.ParseInExList(filters)
	return
}

const runStatusError = "ERROR"

// AggregateErrors collects all errors from the table progress.
func (bp *BackupProgress) AggregateErrors() {
	if bp.Progress == nil || bp.Run.Status != runStatusError {
		return
	}
	for i := range bp.Progress.Hosts {
		for j := range bp.Progress.Hosts[i].Keyspaces {
			for _, t := range bp.Progress.Hosts[i].Keyspaces[j].Tables {
				if t.Error != "" {
					bp.Errors = append(bp.Errors, t.Error)
				}
			}
		}
	}
}

// Render renders *BackupProgress in a tabular format.
func (bp BackupProgress) Render(w io.Writer) error {
	if err := bp.addHeader(w); err != nil {
		return err
	}

	if bp.Progress != nil && bp.Progress.Size > 0 {
		t := table.New()
		bp.addHostProgress(t)
		if _, err := io.WriteString(w, t.String()); err != nil {
			return err
		}
	}

	if bp.Detailed && bp.Progress != nil && bp.Progress.Size > 0 {
		if err := bp.addKeyspaceProgress(w); err != nil {
			return err
		}
	}
	return nil
}

func (bp BackupProgress) addHostProgress(t *table.Table) {
	t.AddRow("Host", "Progress", "Size", "Success", "Deduplicated", "Failed")
	t.AddSeparator()
	for _, h := range bp.Progress.Hosts {
		if bp.hideHost(h.Host) {
			continue
		}
		p := "-"
		if len(h.Keyspaces) > 0 {
			p = FormatUploadProgress(h.Size, h.Uploaded, h.Skipped, h.Failed)
		}
		success := h.Uploaded + h.Skipped
		t.AddRow(h.Host, p,
			ByteCountBinary(h.Size),
			ByteCountBinary(success),
			ByteCountBinary(h.Skipped),
			ByteCountBinary(h.Failed),
		)
	}
	t.SetColumnAlignment(termtables.AlignRight, 1, 2, 3, 4, 5)
}

func (bp BackupProgress) addKeyspaceProgress(w io.Writer) error {
	for _, h := range bp.Progress.Hosts {
		if bp.hideHost(h.Host) {
			continue
		}
		fmt.Fprintf(w, "\nHost: %s\n", h.Host)

		t := table.New("Keyspace", "Table", "Progress", "Size", "Success", "Deduplicated", "Failed", "Started at", "Completed at")
		for i, ks := range h.Keyspaces {
			if bp.hideKeyspace(ks.Keyspace) {
				break
			}
			if i > 0 {
				t.AddSeparator()
			}

			rowAdded := false
			for _, tbl := range ks.Tables {
				startedAt := strfmt.DateTime{}
				completedAt := strfmt.DateTime{}
				if tbl.StartedAt != nil {
					startedAt = *tbl.StartedAt
				}
				if tbl.CompletedAt != nil {
					completedAt = *tbl.CompletedAt
				}
				success := tbl.Uploaded + tbl.Skipped
				t.AddRow(
					ks.Keyspace,
					tbl.Table,
					FormatUploadProgress(tbl.Size,
						tbl.Uploaded,
						tbl.Skipped,
						tbl.Failed),
					ByteCountBinary(tbl.Size),
					ByteCountBinary(success),
					ByteCountBinary(tbl.Skipped),
					ByteCountBinary(tbl.Failed),
					FormatTime(startedAt),
					FormatTime(completedAt),
				)
				rowAdded = true
			}
			if !rowAdded {
				// Separate keyspaces with no table rows
				t.AddRow("-", "-", "-", "-", "-", "-", "-", "-", "-")
			}
		}
		t.SetColumnAlignment(termtables.AlignRight, 2, 3, 4, 5, 6)
		if _, err := w.Write([]byte(t.String())); err != nil {
			return err
		}
	}
	return nil
}

func (bp BackupProgress) hideHost(host string) bool {
	if bp.hostFilter.Size() > 0 {
		return bp.hostFilter.FirstMatch(host) == -1
	}
	return false
}

func (bp BackupProgress) hideKeyspace(keyspace string) bool {
	if bp.keyspaceFilter.Size() > 0 {
		return bp.keyspaceFilter.FirstMatch(keyspace) == -1
	}
	return false
}

var backupProgressTemplate = `{{ if arguments }}Arguments:	{{ arguments }}
{{ end -}}
{{ with .Run }}Status:		{{ status }}
{{- if .Cause }}
Cause:		{{ FormatError .Cause }}

{{- end }}
{{- if not (isZero .StartTime) }}
Start time:	{{ FormatTime .StartTime }}
{{- end -}}
{{- if not (isZero .EndTime) }}
End time:	{{ FormatTime .EndTime }}
{{- end }}
Duration:	{{ FormatDuration .StartTime .EndTime }}
{{ end -}}
{{ with .Progress }}Progress:	{{ if ne .Size 0 }}{{ FormatUploadProgress .Size .Uploaded .Skipped .Failed }}{{else}}-{{ end }}
{{- if ne .SnapshotTag "" }}
Snapshot Tag:	{{ .SnapshotTag }}
{{- end }}
{{ if .Dcs -}}
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
{{ end }}
`

func (bp BackupProgress) addHeader(w io.Writer) error {
	temp := template.Must(template.New("backup_progress").Funcs(template.FuncMap{
		"isZero":               isZero,
		"FormatTime":           FormatTime,
		"FormatDuration":       FormatDuration,
		"FormatError":          FormatError,
		"FormatUploadProgress": FormatUploadProgress,
		"arguments":            bp.arguments,
		"status":               bp.status,
	}).Parse(backupProgressTemplate))
	return temp.Execute(w, bp)
}

// arguments returns task arguments that task was created with.
func (bp BackupProgress) arguments() string {
	return NewCmdRenderer(bp.Task, RenderTypeArgs).String()
}

// status returns task status with optional backup stage.
func (bp BackupProgress) status() string {
	translate := map[string]string{
		"INIT":         "initialising",
		"AWAIT_SCHEMA": "awaiting schema agreement",
		"SNAPSHOT":     "taking snapshot",
		"SCHEMA":       "uploading schema",
		"INDEX":        "indexing files",
		"UPLOAD":       "uploading data",
		"MANIFEST":     "uploading manifest",
		"MIGRATE":      "migrating legacy metadata",
		"PURGE":        "retention",
	}

	stage := translate[bp.Progress.Stage]
	s := bp.Run.Status
	if s != "NEW" && s != "DONE" && stage != "" {
		s += " (" + stage + ")"
	}
	return s
}

// BackupListItems is a []backup.ListItem representation.
type BackupListItems struct {
	items       []*models.BackupListItem
	AllClusters bool
	ShowTables  int
}

const backupListItemTemplate = `Snapshots:
{{- range .SnapshotInfo }}
  - {{ .SnapshotTag }} ({{ if eq .Size 0 }}n/a{{ else }}{{ ByteCountBinary .Size }}{{ end }})
{{- end }}
Keyspaces:
{{- range .Units }}
  - {{ .Keyspace }} {{ FormatTables .Tables .AllTables }}
{{- end }}

`

// Render implements Renderer interface.
func (bl BackupListItems) Render(w io.Writer) error {
	temp := template.Must(template.New("backup_list_items").Funcs(template.FuncMap{
		"FormatTables": func(tables []string, all bool) string {
			return FormatTables(bl.ShowTables, tables, all)
		},
		"ByteCountBinary": ByteCountBinary,
	}).Parse(backupListItemTemplate))

	prev := ""
	for _, i := range bl.items {
		if bl.AllClusters {
			if prev != i.ClusterID {
				prev = i.ClusterID
				fmt.Fprintln(w, "Cluster:", i.ClusterID)
				fmt.Fprintln(w)
			}
		}
		if err := temp.Execute(w, i); err != nil {
			return err
		}
	}
	return nil
}
