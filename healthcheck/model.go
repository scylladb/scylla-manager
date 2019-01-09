// Copyright (C) 2017 ScyllaDB

package healthcheck

const (
	statusUp   = `UP`
	statusDown = `DOWN`
)

// Status represents the status of a particular host
type Status struct {
	DC        string  `json:"dc"`
	Host      string  `json:"host"`
	SSL       bool    `json:"ssl"`
	CQLStatus string  `json:"cql_status"`
	RTT       float64 `json:"cql_rtt_ms"`
}
