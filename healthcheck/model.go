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
	CQLRtt    float64 `json:"cql_rtt_ms"`
	APIStatus string  `json:"api_status"`
	APIRtt    float64 `json:"api_rtt_ms"`
}
