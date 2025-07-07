// Copyright (C) 2017 ScyllaDB

package healthcheck

import (
	"time"

	"github.com/scylladb/scylla-manager/v3/pkg/util/schedules"
)

// Config specifies the healthcheck service configuration.
type Config struct {
	// NodeInfoTTL specifies how long node info should be cached.
	NodeInfoTTL time.Duration `yaml:"node_info_ttl"`
	// MaxTimeout specifies ping timeout for all ping types (CQL, REST, Alternator).
	MaxTimeout time.Duration `yaml:"max_timeout"`
	// CQLPingCron specifies cron expression for scheduling CQL ping.
	CQLPingCron schedules.Cron `yaml:"cql_ping_cron"`
	// RESTPingCron specifies cron expression for scheduling REST ping.
	RESTPingCron schedules.Cron `yaml:"rest_ping_cron"`
	// AlternatorPingCron specifies cron expression for scheduling Alternator ping.
	AlternatorPingCron schedules.Cron `yaml:"alternator_ping_cron"`
	// Deprecated: value is not used anymore
	Probes int `yaml:"probes"`
	// Deprecated: value is not used anymore
	RelativeTimeout time.Duration `yaml:"relative_timeout"`
}

func DefaultConfig() Config {
	return Config{
		MaxTimeout:         1 * time.Second,
		NodeInfoTTL:        5 * time.Minute,
		CQLPingCron:        schedules.MustCron("* * * * *", time.Time{}),
		RESTPingCron:       schedules.MustCron("* * * * *", time.Time{}),
		AlternatorPingCron: schedules.MustCron("* * * * *", time.Time{}),
	}
}
