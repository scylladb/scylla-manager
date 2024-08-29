// Copyright (C) 2017 ScyllaDB

package cpuset

import (
	"bufio"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/pkg/errors"
	"github.com/scylladb/go-set/iset"
)

const (
	// scyllaConfigFile is a file generated by a Scylla configuration tools,
	// it contains additional flags --cpuset and --smp that are passed to
	// scylla-server on start.
	scyllaConfigFile = "/etc/scylla.d/cpuset.conf"
	// cpuSetPattern matches the configuration line in the scyllaConfigFile ex.
	// CPUSET="--cpuset 0 --smp 1".
	cpuSetPattern = `^\s*CPUSET=\s*\"(?:\s*--cpuset(?:\s*|=)(?P<cpuset>\d+(?:[-,]\d+)*))?(?:\s*--smp(?:\s*|=)(?P<smp>\d+))?"`
)

// ErrNoCPUSetConfig is returned in presence of an empty cpuset.conf file.
var ErrNoCPUSetConfig = errors.New("no CPUSET configuration")

// ParseScyllaConfigFile returns a list of busy CPUs based on /etc/scylla.d/cpuset.conf
// contents.
func ParseScyllaConfigFile() ([]int, error) {
	return parseConfigFile(scyllaConfigFile)
}

func parseConfigFile(name string) ([]int, error) {
	file, err := os.Open(name)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var groups []string
	p := regexp.MustCompile(cpuSetPattern)
	s := bufio.NewScanner(file)
	for s.Scan() {
		if m := p.FindStringSubmatch(s.Text()); m != nil {
			groups = m
		}
	}
	if err := s.Err(); err != nil {
		return nil, err
	}
	if groups == nil {
		return nil, ErrNoCPUSetConfig // nolint: errorlint
	}

	idx := 0
	for i, n := range p.SubexpNames() {
		if n == "cpuset" {
			idx = i
			break
		}
	}

	if groups[idx] == "" {
		return nil, errors.New("CPUSET configuration is missing cpuset flag")
	}

	return parseCPUSet(groups[idx])
}

func parseCPUSet(s string) ([]int, error) {
	cpus := iset.New()

	for _, g := range strings.Split(s, ",") {
		v := strings.Split(g, "-")
		start, err := strconv.ParseInt(v[0], 10, 64)
		if err != nil {
			return nil, err
		}
		end, err := strconv.ParseInt(v[len(v)-1], 10, 64)
		if err != nil {
			return nil, err
		}
		for i := start; i <= end; i++ {
			cpus.Add(int(i))
		}
	}

	l := cpus.List()
	sort.Ints(l)
	return l, nil
}