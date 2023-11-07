// Copyright (C) 2017 ScyllaDB

package scyllaclient_test

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/scylladb/go-log"
	"github.com/scylladb/scylla-manager/v3/pkg/rclone"
	. "github.com/scylladb/scylla-manager/v3/pkg/testutils"
	"go.uber.org/zap/zapcore"
)

func TestMain(m *testing.M) {
	rth.setup()
	ret := m.Run()
	rth.tearDown()

	os.Exit(ret)
}

var rth rcloneTestHelper

type rcloneTestHelper struct {
	tmpDir string
}

func (r *rcloneTestHelper) setup() {
	d, err := ioutil.TempDir("", "scylla-manager-rclone")
	if err != nil {
		panic(err)
	}
	r.tmpDir = d

	rclone.RedirectLogPrint(log.NewDevelopmentWithLevel(zapcore.InfoLevel).Named("rclone"))
	// Disable certificate check for scyllaclient package integration tests.
	// Rclone server here is started on the localhost.
	// Minio with custom CA certs is verified by integration-tests from other packages.
	globalOptions := rclone.DefaultGlobalOptions()
	globalOptions.InsecureSkipVerify = true
	rclone.InitFsConfigWithOptions(globalOptions)
	rclone.MustRegisterLocalDirProvider("dev", "", "/dev")
	rclone.MustRegisterLocalDirProvider("tmp", "", r.tmpDir)
	rclone.MustRegisterLocalDirProvider("rclonetest", "", "testdata/rclone")
	rclone.MustRegisterLocalDirProvider("rclonejail", "", "testdata/rclone/jail")
	rclone.MustRegisterS3Provider(S3Credentials())
}

func (r *rcloneTestHelper) tearDown() {
	os.RemoveAll(r.tmpDir)
}
