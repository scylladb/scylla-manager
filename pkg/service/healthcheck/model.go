// Copyright (C) 2017 ScyllaDB

package healthcheck

import (
	"github.com/pkg/errors"
	"github.com/scylladb/go-set/strset"
	"github.com/scylladb/scylla-manager/v3/pkg/scyllaclient"
)

const (
	statusUp   = `UP`
	statusDown = `DOWN`

	statusError        = `ERROR`
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
	AlternatorCause  string  `json:"alternator_cause"`
	CQLStatus        string  `json:"cql_status"`
	CQLRtt           float64 `json:"cql_rtt_ms"`
	CQLCause         string  `json:"cql_cause"`
	RESTStatus       string  `json:"rest_status"`
	RESTRtt          float64 `json:"rest_rtt_ms"`
	RESTCause        string  `json:"rest_cause"`
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

// Mode specifies the type of health-check.
type Mode string

// Mode enumeration.
const (
	CQLMode        = Mode("cql")
	RESTMode       = Mode("rest")
	AlternatorMode = Mode("alternator")
)

func (m Mode) String() string {
	return string(m)
}

func (m Mode) MarshalText() (text []byte, err error) {
	return []byte(m), nil
}

var validModes = strset.New(CQLMode.String(), RESTMode.String(), AlternatorMode.String())

func (m *Mode) UnmarshalText(text []byte) error {
	s := string(text)
	if !validModes.Has(string(text)) {
		return errors.Errorf("unsupported mode %s", s)
	}
	*m = Mode(s)
	return nil
}

type taskProperties struct {
	Mode Mode `json:"mode"`
}
