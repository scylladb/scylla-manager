// Copyright (C) 2017 ScyllaDB

package managerclient

import (
	"bytes"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"
)

// CmdRenderType defines CmdRenderer output type.
type CmdRenderType int

const (
	// RenderAll render entire task command.
	RenderAll CmdRenderType = iota
	// RenderArgs render only args of the command.
	RenderArgs
	// RenderTypeArgs render only args specific to the type of the task.
	RenderTypeArgs
)

// CmdRenderer know how to transform task into a command line that creates it.
type CmdRenderer struct {
	buf  *bytes.Buffer
	err  error
	task *Task
	rt   CmdRenderType
}

// NewCmdRenderer creates new CmdRenderer.
func NewCmdRenderer(t *Task, rt CmdRenderType) *CmdRenderer {
	return &CmdRenderer{
		buf:  bytes.NewBuffer(nil),
		task: t,
		rt:   rt,
	}
}

func (rc *CmdRenderer) String() string {
	buf := bytes.NewBuffer(nil)
	if err := rc.Render(buf); err != nil {
		return ""
	}
	return buf.String()
}

func (rc *CmdRenderer) writeArg(in ...string) {
	in = append(in, " ") // Add spacing between arguments
	for _, s := range in {
		if rc.err != nil {
			return
		}
		_, err := rc.buf.WriteString(s)
		if err != nil {
			rc.err = err
		}
	}
}

type transformer func(val string) string

func quoted(val string) string {
	return "'" + val + "'"
}

func byteCount(val string) string {
	i, err := strconv.ParseInt(val, 10, 64)
	if err != nil {
		return "error"
	}
	return FormatSizeSuffix(i)
}

// writeProp adds a map from argument to the task property.
// Transformers will be applied to the value in the provided order.
func (rc *CmdRenderer) writeProp(arg, prop string, transformers ...transformer) {
	if rc.task.Properties == nil {
		return
	}
	p, ok := rc.task.Properties.(map[string]interface{})
	if !ok {
		return
	}
	v, ok := p[prop]
	if !ok || v == nil {
		return
	}
	switch val := v.(type) {
	case []interface{}:
		if len(val) == 0 {
			return
		}
		tmp := make([]string, len(val))
		for i := range tmp {
			tmp[i] = val[i].(string)
		}
		out := strings.Join(tmp, ",")
		for i := range transformers {
			out = transformers[i](out)
		}
		rc.writeArg(arg, " ", out)
	case []string:
		if len(val) == 0 {
			return
		}
		out := strings.Join(val, ",")
		for i := range transformers {
			out = transformers[i](out)
		}
		rc.writeArg(arg, " ", out)
	case bool:
		rc.writeArg(arg)
	default:
		out := fmt.Sprintf("%v", v)
		for i := range transformers {
			out = transformers[i](out)
		}
		rc.writeArg(arg, " ", out)
	}
}

// Render implements Renderer interface.
func (rc CmdRenderer) Render(w io.Writer) error {
	switch rc.rt {
	case RenderAll:
		rc.writeArg("sctool", " ", rc.task.Type)
		fallthrough
	case RenderArgs:
		rc.writeArg("--cluster", " ", rc.task.ClusterID)
		if rc.task.Schedule.Cron != "" {
			rc.writeArg("--cron", " ", quoted(rc.task.Schedule.Cron))
		}
		if rc.task.Schedule.Interval != "" {
			rc.writeArg("--interval", " ", rc.task.Schedule.Interval)
		}
		if !time.Time(rc.task.Schedule.StartDate).IsZero() {
			rc.writeArg("--start-date", " ", rc.task.Schedule.StartDate.String())
		}
		rc.writeArg("--num-retries", " ", fmt.Sprintf("%d", rc.task.Schedule.NumRetries))
		fallthrough
	case RenderTypeArgs:
		switch rc.task.Type {
		case BackupTask:
			rc.writeProp("-K", "keyspace", quoted)
			rc.writeProp("--dc", "dc", quoted)
			rc.writeProp("-L", "location")
			rc.writeProp("--retention", "retention")
			rc.writeProp("--rate-limit", "rate_limit")
			rc.writeProp("--snapshot-parallel", "snapshot_parallel", quoted)
			rc.writeProp("--upload-parallel", "upload_parallel", quoted)
			rc.writeProp("--purge-only", "purge_only")
		case RepairTask:
			rc.writeProp("-K", "keyspace", quoted)
			rc.writeProp("--dc", "dc", quoted)
			rc.writeProp("--host", "host", quoted)
			rc.writeProp("--fail-fast", "fail_fast")
			rc.writeProp("--intensity", "intensity")
			rc.writeProp("--parallel", "parallel")
			rc.writeProp("--small-table-threshold", "small_table_threshold", byteCount)
		case ValidateBackupTask:
			rc.writeProp("-L", "location")
			rc.writeProp("--delete-orphaned-files", "delete_orphaned_files")
			rc.writeProp("--parallel", "parallel")
		}
	}

	return rc.flush(w)
}

func (rc *CmdRenderer) flush(wr io.Writer) error {
	if rc.err != nil {
		return rc.err
	}
	_, err := wr.Write(bytes.TrimSpace(rc.buf.Bytes()))
	return err
}
