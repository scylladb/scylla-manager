// Copyright (C) 2025 ScyllaDB

package one2onerestore

import (
	"bufio"
	"bytes"
	"os"

	"github.com/pkg/errors"
)

// implements pflag.Value interface.
type nodesMapping []nodeMapping

type nodeMapping struct {
	Source node `json:"source"`
	Target node `json:"target"`
}

type node struct {
	DC     string `json:"dc"`
	Rack   string `json:"rack_id"`
	HostID string `json:"host_id"`
}

func (m *nodesMapping) Set(filePath string) error {
	fd, err := os.Open(filePath)
	if err != nil {
		return errors.Wrap(err, "open a file")
	}
	defer fd.Close()

	scanner := bufio.NewScanner(fd)
	scanner.Split(bufio.ScanLines)
	for scanner.Scan() {
		lineBytes := scanner.Bytes()
		mappingParts := bytes.Split(lineBytes, []byte("="))
		if len(mappingParts) != 2 {
			return errors.New("source and target mappings must be split by '='")
		}
		sourceNode, err := parseNode(mappingParts[0])
		if err != nil {
			return err
		}
		targetNode, err := parseNode(mappingParts[1])
		if err != nil {
			return err
		}
		*m = append(*m, nodeMapping{
			Source: sourceNode,
			Target: targetNode,
		})
	}
	if err := scanner.Err(); err != nil {
		return errors.Wrap(scanner.Err(), "scan lines")
	}
	if err := m.validate(); err != nil {
		*m = nil
		return errors.Wrap(err, "validation")
	}
	return nil
}

func parseNode(rawNode []byte) (node, error) {
	nodeParts := bytes.Split(rawNode, []byte(":"))
	if len(nodeParts) != 3 {
		return node{}, errors.New("invalid node format, must be <dc>:<rack>:<host_id>")
	}
	return node{
		DC:     string(bytes.TrimSpace(nodeParts[0])),
		Rack:   string(bytes.TrimSpace(nodeParts[1])),
		HostID: string(bytes.TrimSpace(nodeParts[2])),
	}, nil
}

func (m nodesMapping) validate() error {
	if len(m) == 0 {
		return errors.Errorf("node mappings can't be empty")
	}
	var (
		sourceDCCount = map[string]int{}
		targetDCCount = map[string]int{}

		sourceRackCount = map[string]int{}
		targetRackCount = map[string]int{}
	)
	for _, nodeMapping := range m {
		s, t := nodeMapping.Source, nodeMapping.Target

		sourceDCCount[s.DC]++
		targetDCCount[t.DC]++

		sourceRackCount[s.DC+s.Rack]++
		targetRackCount[t.DC+t.Rack]++

		if sourceDCCount[s.DC] != targetDCCount[t.DC] {
			return errors.Errorf("source and target cluster has different nodes per DC count")
		}

		if sourceRackCount[s.DC+s.Rack] != targetRackCount[t.DC+t.Rack] {
			return errors.Errorf("source and target cluster has different nodes per Rack count")
		}
	}
	return nil
}

func (m *nodesMapping) String() string {
	return ""
}

func (m *nodesMapping) Type() string {
	return "nodes-mapping"
}
