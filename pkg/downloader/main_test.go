// Copyright (C) 2017 ScyllaDB

package downloader

import (
	"os"
	"testing"

	"github.com/scylladb/go-log"
	"github.com/scylladb/scylla-manager/v3/pkg/rclone"
	"go.uber.org/zap/zapcore"
)

func TestMain(m *testing.M) {
	setupRclone()
	os.Exit(m.Run())
}

func setupRclone() {
	rclone.RedirectLogPrint(log.NewDevelopmentWithLevel(zapcore.ErrorLevel).Named("rclone"))
	rclone.InitFsConfig()
	rclone.MustRegisterLocalDirProvider("testdata", "", "testdata")
}
