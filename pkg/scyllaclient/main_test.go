// Copyright (C) 2017 ScyllaDB

package scyllaclient_test

import (
	"os"
	"testing"

	"github.com/scylladb/go-log"
	"github.com/scylladb/scylla-manager/pkg/rclone"
	. "github.com/scylladb/scylla-manager/pkg/testutils"
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
	rclone.MustRegisterLocalDirProvider("dev", "", "/dev")
	rclone.MustRegisterLocalDirProvider("tmp", "", "/tmp")
	rclone.MustRegisterLocalDirProvider("rclonetest", "", rootDir)
	rclone.MustRegisterLocalDirProvider("rclonejail", "", "testdata/rclone/jail")
	rclone.MustRegisterS3Provider(S3Credentials())
}
