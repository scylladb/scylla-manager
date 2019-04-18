// Copyright (C) 2017 ScyllaDB

package osutil

import (
	"bufio"
	"bytes"
	"io/ioutil"
	"os"
	"os/exec"
	"regexp"
	"strings"

	"github.com/scylladb/mermaid/uuid"
)

// Distro is string representation of the linux distribution.
type Distro string

const (
	// Amazon Linux distribution id.
	Amazon Distro = "amzn"
	// Centos Linux distribution id.
	Centos Distro = "centos"
	// Debian Linux distribution id.
	Debian Distro = "debian"
	// Fedora Linux distribution id.
	Fedora Distro = "fedora"
	// Rehl Red Hat Linux distribution id.
	Rehl Distro = "rehl"
	// Ubuntu Linux distribution id.
	Ubuntu Distro = "ubuntu"
	// Unknown distro.
	Unknown Distro = "unknown"
)

// osReleaseFile used for override in tests.
var osReleaseFile = "/etc/os-release"

// LinuxDistro returns Linux distribution id.
// Common ids are defined as constants.
// Unknown is returned if distro can't be detected.
func LinuxDistro() Distro {
	f, err := os.Open(osReleaseFile)
	if err != nil {
		return Unknown
	}
	defer f.Close()
	s := bufio.NewScanner(f)
	release := "unknown"
	for s.Scan() {
		l := s.Text()
		if strings.HasPrefix(l, "ID=") {
			release = strings.Trim(l[3:], `"`)
			break
		}
	}
	return Distro(release)
}

// dockerEnvFile used for override in tests.
var dockerEnvFile = "/.dockerenv"

// cGroupFile used for override in tests.
var cGroupFile = "/proc/1/cgroup"

// Docker returns true if application is running inside docker environment.
// Tests presence of "/.dockerenv" file or "docker|lxc" in /proc/1/cgroup.
func Docker() bool {
	if f, err := os.Open(dockerEnvFile); err == nil {
		f.Close()
		return true
	}
	if f, err := os.Open(cGroupFile); err == nil {
		defer f.Close()
		b, err := ioutil.ReadAll(f)
		if err != nil {
			return false
		}
		if bytes.Contains(b, []byte("docker")) || bytes.Contains(b, []byte("lxc")) {
			return true
		}
	}

	return false
}

// macUUIDFile used for override in tests.
var macUUIDFile = "/var/tmp/scylla-manager-macuuid"

// MacUUID returns unique id of the machine that application is running on.
// Implementation is using file to generate this value.
// If the file system is/ not persistent like in Docker it will be different
// every time this file is removed.
func MacUUID() uuid.UUID {
	f, err := os.Open(macUUIDFile)
	if err != nil {
		macUUID := uuid.NewTime()
		f, err := os.OpenFile(macUUIDFile, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, os.ModePerm)
		if err != nil {
			return uuid.Nil
		}
		n, err := f.Write([]byte(macUUID.String()))
		if err == nil && n < len(macUUID.String()) {
			return uuid.Nil
		}
		if err := f.Close(); err != nil {
			return uuid.Nil
		}
		return macUUID
	}
	defer f.Close()
	out, err := ioutil.ReadAll(f)
	if err != nil {
		return uuid.Nil
	}
	id, err := uuid.Parse(string(out))
	if err != nil {
		return uuid.Nil
	}
	return id
}

var (
	// regExecCommand used for override in tests.
	regExecCommand = func(name string, args ...string) ([]byte, error) {
		cmd := exec.Command(name, args...)
		return cmd.CombinedOutput()
	}
	// Sample repository URL:
	// https://repositories.scylladb.com/scylla/downloads/scylladb/f3d53efa-ee7e-4f4e-9d4b-7ee6b9ea48da/scylla-manager/rpm/centos/scylladb-manager-1.3
	idRegex = regexp.MustCompile("(?s)scylladb/([a-z0-9-]+)/scylla-manager")
)

// RegUUID extracts registration id from scylla-manager package repositories.
func RegUUID() uuid.UUID {
	var out []byte
	var err error
	switch LinuxDistro() {
	case Ubuntu, Debian:
		out, err = regExecCommand("apt-cache", "policy", "scylla-manager-server")
	case Amazon, Rehl, Centos, Fedora:
		out, err = regExecCommand("repoquery", "--location", "scylla-manager-server")
	default:
		return uuid.Nil
	}
	if err != nil {
		return uuid.Nil
	}
	match := idRegex.FindStringSubmatch(string(out))
	if len(match) < 2 {
		return uuid.Nil
	}
	id, err := uuid.Parse(match[1])
	if err != nil {
		return uuid.Nil
	}
	return id
}
