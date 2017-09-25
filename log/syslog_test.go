// Copyright (C) 2017 ScyllaDB

package log

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log/syslog"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func makeInt64Field(key string, val int) zapcore.Field {
	return zapcore.Field{Type: zapcore.Int64Type, Integer: int64(val), Key: key}
}

// man journalctl
// syslog(3), i.e.  "emerg" (0), "alert" (1), "crit" (2), "err" (3), "warning" (4), "notice" (5), "info" (6), "debug" (7).

func TestSyslogCoreCore(t *testing.T) {
	w, err := syslog.New(syslog.LOG_DAEMON, "zapcore-test-syslogCore")
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	// Drop timestamps for simpler assertSyslogCorens (timestamp encoding is tested
	// elsewhere).
	cfg := zap.NewProductionEncoderConfig()
	cfg.LevelKey = ""
	cfg.TimeKey = ""

	core := NewSyslogCore(
		zapcore.NewJSONEncoder(cfg),
		w,
		zapcore.InfoLevel,
	).With([]zapcore.Field{makeInt64Field("k", 1)})

	if err := core.Sync(); err != nil {
		t.Fatal(err)
	}

	if ce := core.Check(zapcore.Entry{Level: zapcore.DebugLevel, Message: "debug"}, nil); ce != nil {
		ce.Write(makeInt64Field("k", 2))
	}
	if ce := core.Check(zapcore.Entry{Level: zapcore.InfoLevel, Message: "info"}, nil); ce != nil {
		ce.Write(makeInt64Field("k", 3))
	}
	if ce := core.Check(zapcore.Entry{Level: zapcore.WarnLevel, Message: "warn"}, nil); ce != nil {
		ce.Write(makeInt64Field("k", 4))
	}

	if err := core.(*SyslogCore).Sync(); err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)

	logged, err := readSyslog("zapcore-test-syslogCore")
	if err != nil {
		t.Fatal(err)
	}

	e := []*sysctlMsg{
		{
			PRIORITY:         "6",
			SYSLOGIDENTIFIER: "zapcore-test-syslogCore",
			MESSAGE:          `{"msg":"info","k":1,"k":3}`,
		},
		{
			PRIORITY:         "4",
			SYSLOGIDENTIFIER: "zapcore-test-syslogCore",
			MESSAGE:          `{"msg":"warn","k":1,"k":4}`,
		},
	}

	if diff := cmp.Diff(logged, e); diff != "" {
		t.Fatal(diff)
	}
}

func readSyslog(tag string) ([]*sysctlMsg, error) {
	buf := bytes.NewBuffer(nil)
	cmd := exec.Command("journalctl", "-t", tag, fmt.Sprintf("_PID=%d", os.Getpid()), "-o", "json")
	cmd.Stdout = buf
	if err := cmd.Run(); err != nil {
		return nil, err
	}

	var logs []*sysctlMsg
	for {
		b, err := buf.ReadBytes('\n')
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
		m := new(sysctlMsg)
		if err := json.Unmarshal(b, m); err != nil {
			return nil, err
		}
		logs = append(logs, m)
	}
	return logs, nil
}

type sysctlMsg struct {
	PRIORITY         string `json:"PRIORITY"`
	SYSLOGIDENTIFIER string `json:"SYSLOG_IDENTIFIER"`
	MESSAGE          string `json:"MESSAGE"`
}
