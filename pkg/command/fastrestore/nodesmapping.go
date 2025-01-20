// Copyright (C) 2025 ScyllaDB

package fastrestore

import (
	"bufio"
	"bytes"
	"os"

	"github.com/pkg/errors"
)

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

	return errors.Wrap(scanner.Err(), "scan lines")
}

func parseNode(rawNode []byte) (node, error) {
	nodeParts := bytes.Split(rawNode, []byte(":"))
	if len(nodeParts) != 3 {
		return node{}, errors.New("Invalid node format, must be <dc>:<rack>:<host_id>")
	}
	return node{
		DC:     string(bytes.TrimSpace(nodeParts[0])),
		Rack:   string(bytes.TrimSpace(nodeParts[1])),
		HostID: string(bytes.TrimSpace(nodeParts[2])),
	}, nil
}

func (m *nodesMapping) String() string {
	return ""
}

func (m *nodesMapping) Type() string {
	return "nodes-mapping"
}
