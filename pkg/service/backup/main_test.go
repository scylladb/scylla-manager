// Copyright (C) 2017 ScyllaDB

package backup

import (
	"os"
	"path"
	"testing"

	"github.com/scylladb/go-log"
	"github.com/scylladb/scylla-manager/pkg/rclone"
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

	rclone.RedirectLogPrint(log.NewDevelopmentWithLevel(zapcore.InfoLevel).Named("rclone"))
	rclone.InitFsConfig()

	rclone.MustRegisterLocalDirProvider("testdata", "", path.Join(rootDir, "testdata"))
}
