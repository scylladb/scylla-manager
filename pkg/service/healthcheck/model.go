// Copyright (C) 2017 ScyllaDB

package healthcheck

import (
	"github.com/scylladb/scylla-manager/pkg/scyllaclient"
)

const (
	statusUp           = `UP`
	statusDown         = `DOWN`
	statusTimeout      = `TIMEOUT`
	statusUnauthorized = `UNAUTHORIZED`
	statusHTTP         = `HTTP`
)

// NodeStatus represents the status of a particular node.
type NodeStatus struct {
	Datacenter       string  `json:"dc"`
	HostID           string  `json:"host_id"`
	Host             string  `json:"host"`
	Status           string  `json:"status"`
	SSL              bool    `json:"ssl"`
	AlternatorStatus string  `json:"alternator_status"`
	AlternatorRtt    float64 `json:"alternator_rtt_ms"`
	CQLStatus        string  `json:"cql_status"`
	CQLRtt           float64 `json:"cql_rtt_ms"`
	RESTStatus       string  `json:"rest_status"`
	RESTRtt          float64 `json:"rest_rtt_ms"`
	TotalRAM         int64   `json:"total_ram"`
	Uptime           int64   `json:"uptime"`
	CPUCount         int64   `json:"cpu_count"`
	ScyllaVersion    string  `json:"scylla_version"`
	AgentVersion     string  `json:"agent_version"`
}

func makeNodeStatus(src []scyllaclient.NodeStatusInfo) []NodeStatus {
	dst := make([]NodeStatus, len(src))
	for i := range src {
		dst[i].Datacenter = src[i].Datacenter
		dst[i].HostID = src[i].HostID
		dst[i].Host = src[i].Addr
		dst[i].Status = src[i].Status.String() + src[i].State.String()
	}
	return dst
}
