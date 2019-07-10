// Copyright (C) 2017 ScyllaDB

package rclone

import (
	"fmt"
	"time"

	"github.com/ncw/rclone/fs"
	"github.com/scylladb/mermaid"
)

// SetDefaultConfig sets default config values expected for correct agent
// behaviour.
func SetDefaultConfig() {
	fs.Config.LogLevel = fs.LogLevelInfo // Default logging level
	fs.Config.IgnoreErrors = true        // Delete even if there are I/O errors
	fs.Config.SizeOnly = true            // Only use size to compare files
	fs.Config.NoUpdateModTime = true     // Don't update destination mod-time if files identical
	fs.Config.UserAgent = fmt.Sprintf(
		"Scylla Manager Agent %s", mermaid.Version()) // Set proper agent for backend clients
	fs.Config.RcJobExpireDuration = 1 * time.Hour   // Expire async jobs after this duration
	fs.Config.RcJobExpireInterval = 1 * time.Minute // Check for expired async jobs at this interval
	fs.Config.LowLevelRetries = 2                   // How many times to retry low level operations like copy file
	fs.Config.MaxStatsGroups = 1000                 // How many stat groups to keep in memory
}
