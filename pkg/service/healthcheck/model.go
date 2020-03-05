// Copyright (C) 2017 ScyllaDB

package healthcheck

import "github.com/scylladb/mermaid/pkg/scyllaclient"

const (
	statusUp   = `UP`
	statusDown = `DOWN`
)

// NodeStatus represents the status of a particular node.
type NodeStatus struct {
	Datacenter string  `json:"dc"`
	HostID     string  `json:"host_id"`
	Host       string  `json:"host"`
	Status     string  `json:"status"`
	SSL        bool    `json:"ssl"`
	CQLStatus  string  `json:"cql_status"`
	CQLRtt     float64 `json:"cql_rtt_ms"`
	RESTStatus string  `json:"rest_status"`
	RESTRtt    float64 `json:"rest_rtt_ms"`
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
