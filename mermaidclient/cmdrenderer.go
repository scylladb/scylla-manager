package mermaidclient

import (
	"bytes"
	"fmt"
	"io"
	"strings"
)

const (
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
		s = strings.Replace(s, "*", `\*`, -1)
		_, err := io.WriteString(rc.buf, s)
		if err != nil {
			rc.err = err
		}
	}
}

func (rc *CmdRenderer) writeProp(arg, prop string) {
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
		rc.writeArg(arg, " ", strings.Join(tmp, ","))
	case []string:
		rc.writeArg(arg, " ", strings.Join(val, ","))
	case bool:
		rc.writeArg(arg)
	default:
		rc.writeArg(arg, " ", fmt.Sprintf("%v", v))
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
		if rc.task.Type == repairTaskType {
			rc.writeProp("-K", "keyspace")
			rc.writeProp("--dc", "dc")
			rc.writeProp("--host", "host")
			rc.writeProp("--with-hosts", "with_hosts")
			rc.writeProp("--fail-fast", "fail_fast")
			rc.writeProp("--token-ranges", "token_ranges")
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
