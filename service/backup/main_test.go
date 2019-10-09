// Copyright (C) 2017 ScyllaDB

package backup

import (
	"os"
	"path"
	"testing"

	"github.com/scylladb/go-log"
	"github.com/scylladb/mermaid/rclone"
	"github.com/scylladb/mermaid/rclone/rcserver"
	"go.uber.org/zap/zapcore"
)

func TestMain(m *testing.M) {
	setupRclone()
	os.Exit(m.Run())
}

func setupRclone() {
	rootDir, err := os.Getwd()
	if err != nil {
		panic(err)
	}

	rclone.SetDefaultConfig()
	rclone.RedirectLogPrint(log.NewDevelopmentWithLevel(zapcore.InfoLevel).Named("rclone"))

	rcserver.MustRegisterInMemoryConf()
	rcserver.MustRegisterLocalDirProvider("walkertest", "", path.Join(rootDir, "testdata", "walker", "simple"))
	rcserver.MustRegisterLocalDirProvider("listtest", "", path.Join(rootDir, "testdata", "walker", "list"))
}
