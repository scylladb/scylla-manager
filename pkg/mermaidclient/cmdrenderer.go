// Copyright (C) 2017 ScyllaDB

package mermaidclient

import (
	"bytes"
	"fmt"
	"io"
	"strings"
)

const (
	backupTaskType = "backup"
	repairTaskType = "repair"
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
		_, err := io.WriteString(rc.buf, s)
		if err != nil {
			rc.err = err
		}
	}
}

// writeProp adds a map from argument to the task property.
// If quoted is true property value will be single quoted.
func (rc *CmdRenderer) writeProp(arg, prop string, quoted bool) {
	if rc.task.Properties == nil {
		return
	}
	p, ok := rc.task.Properties.(map[string]interface{})
	if !ok {
		return
	}
	v, ok := p[prop]
	if !ok {
		return
	}
	switch val := v.(type) {
	case []interface{}:
		tmp := make([]string, len(val))
		for i := range tmp {
			tmp[i] = val[i].(string)
		}
		out := strings.Join(tmp, ",")
		if quoted {
			out = "'" + out + "'"
		}
		rc.writeArg(arg, " ", out)
	case []string:
		out := strings.Join(val, ",")
		if quoted {
			out = "'" + out + "'"
		}
		rc.writeArg(arg, " ", out)
	case bool:
		rc.writeArg(arg)
	default:
		out := fmt.Sprintf("%v", v)
		if quoted {
			out = "'" + out + "'"
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
		rc.writeArg("--start-date", " ", rc.task.Schedule.StartDate.String())
		rc.writeArg("--num-retries", " ", fmt.Sprintf("%d", rc.task.Schedule.NumRetries))
		if rc.task.Schedule.Interval != "" {
			rc.writeArg("--interval", " ", rc.task.Schedule.Interval)
		}
		fallthrough
	case RenderTypeArgs:
		switch rc.task.Type {
		case backupTaskType:
			rc.writeProp("-K", "keyspace", true)
			rc.writeProp("--dc", "dc", true)
			rc.writeProp("-L", "location", false)
			rc.writeProp("--retention", "retention", false)
			rc.writeProp("--rate-limit", "rate_limit", false)
			rc.writeProp("--snapshot-parallel", "snapshot_parallel", false)
			rc.writeProp("--upload-parallel", "upload_parallel", false)
		case repairTaskType:
			rc.writeProp("-K", "keyspace", true)
			rc.writeProp("--dc", "dc", true)
			rc.writeProp("--host", "host", false)
			rc.writeProp("--with-hosts", "with_hosts", false)
			rc.writeProp("--fail-fast", "fail_fast", false)
			rc.writeProp("--token-ranges", "token_ranges", false)
			rc.writeProp("--intensity", "intensity", false)
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
