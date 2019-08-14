// Copyright (C) 2017 ScyllaDB

package rclone

import (
	"fmt"
	"time"

	"github.com/rclone/rclone/fs"
	"github.com/scylladb/mermaid"
)

// SetDefaultConfig sets default config values expected for correct agent
// behaviour.
func SetDefaultConfig() {
	// Don't use JSON log format in logging.
	fs.Config.UseJSONLog = false
	// Pass all logs, our logger decides which one to print.
	fs.Config.LogLevel = fs.LogLevelDebug
	// Delete even if there are I/O errors.
	fs.Config.IgnoreErrors = true
	// Only use size to compare files.
	fs.Config.SizeOnly = true
	// Don't update destination mod-time if files identical.
	fs.Config.NoUpdateModTime = true
	// Set proper agent for backend clients.
	fs.Config.UserAgent = fmt.Sprintf("Scylla Manager Agent %s", mermaid.Version())
	// Expire async jobs after this duration.
	fs.Config.RcJobExpireDuration = 1 * time.Hour
	// Check for expired async jobs at this interval.
	fs.Config.RcJobExpireInterval = 1 * time.Minute
	// How many times to retry low level operations like copy file.
	fs.Config.LowLevelRetries = 2
	// How many stat groups to keep in memory.
	fs.Config.MaxStatsGroups = 1000
}
