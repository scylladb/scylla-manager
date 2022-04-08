// Copyright (C) 2017 ScyllaDB

package osutil

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"testing"

	"github.com/scylladb/scylla-manager/v3/pkg/util/uuid"
)

func TestLinuxDistro(t *testing.T) {
	table := []struct {
		Name     string
		File     string
		Expected Distro
	}{
		{"supported distro", "testdata/linux_distro/supported", Ubuntu},
		{"supported quoted", "testdata/linux_distro/supported-quoted", Centos},
		{"unsupported distro", "testdata/linux_distro/unsupported", Distro("arch")},
		{"empty", "testdata/linux_distro/empty", Unknown},
		{"error", "testdata/linux_distro", Unknown},
	}
	for i := range table {
		test := table[i]

		t.Run(test.Name, func(t *testing.T) {
			defer func(old string) { osReleaseFile = old }(osReleaseFile)
			osReleaseFile = test.File
			dist := LinuxDistro()
			if dist != test.Expected {
				t.Errorf("LinuxDistro() = %q, expected %q", dist, test.Expected)
			}
		})
	}
}

func TestDocker(t *testing.T) {
	table := []struct {
		Name       string
		DockerFile string
		CgroupFile string
		Expected   bool
	}{
		{"dockerenv present", "testdata/docker/dockerenv", "", true},
		{"cgroup docker", "", "testdata/docker/cgroup-docker", true},
		{"cgroup lxc", "", "testdata/docker/cgroup-lxc", true},
		{"not docker error files", "", "", false},
		{"not docker missing cgroup", "", "testdata/docker/empty", false},
	}
	for i := range table {
		test := table[i]

		t.Run(test.Name, func(t *testing.T) {
			defer func(oldEnv, oldCgr string) {
				dockerEnvFile = oldEnv
				cGroupFile = oldCgr
			}(dockerEnvFile, cGroupFile)
			dockerEnvFile = test.DockerFile
			cGroupFile = test.CgroupFile
			got := Docker()
			if got != test.Expected {
				t.Errorf("Docker() = %v, expected %v", got, test.Expected)
			}
		})
	}
}

func TestMacUUID(t *testing.T) {
	defer func(old string) { macUUIDFile = old }(macUUIDFile)
	f, err := ioutil.TempFile("", "scylla-manager-testing-macuuid")
	if err != nil {
		log.Fatal(err)
	}
	name := f.Name()
	f.Close()
	os.Remove(name)
	macUUIDFile = name
	got := MacUUID()
	if got == uuid.Nil {
		t.Errorf("MacUUID() = %v, expected not nil", got)
	}
	got2 := MacUUID()
	if got2 != got {
		t.Errorf("MacUUID() = %v, expected %v", got2, got)
	}
}

const regUUID = "41e0c292-6044-11e9-940a-c85b76fba4f6"

func TestRegUUID(t *testing.T) {
	defer func(old string) { osReleaseFile = old }(osReleaseFile)
	defer func(old func(name string, args ...string) ([]byte, error)) { regExecCommand = old }(regExecCommand)
	regExecCommand = fakeExecCommand

	t.Run("ubuntu", func(t *testing.T) {
		osReleaseFile = "testdata/reguuid/ubuntu"
		got := RegUUID()
		if got != uuid.MustParse(regUUID) {
			t.Errorf("RegUUID() = %v, expected %v", got, regUUID)
		}
	})

	t.Run("centos", func(t *testing.T) {
		osReleaseFile = "testdata/reguuid/centos"
		got := RegUUID()
		if got != uuid.MustParse(regUUID) {
			t.Errorf("RegUUID() = %v, expected %v", got, regUUID)
		}
	})
}

func fakeExecCommand(command string, args ...string) ([]byte, error) {
	out := []byte(fmt.Sprintf("https://repositories.scylladb.com/scylla/downloads/scylladb/%s/scylla-manager/rpm/centos/scylladb-manager-1.3", regUUID))
	return out, nil
}
