// Copyright (C) 2017 ScyllaDB

package rclone

import (
	"fmt"

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
	// Don't use readahead buffering in accounting. We enable readahead in
	// kernel with SEQENTIAL read mode, adding this makes things slower and
	// consumes more memory.
	fs.Config.BufferSize = 0
	// Delete even if there are I/O errors.
	fs.Config.IgnoreErrors = true
	// Do not compare hash post upload, prevents from calculating hashes in
	// rclone versions >= 1.48.
	fs.Config.IgnoreChecksum = true
	// Only use size to compare files.
	fs.Config.SizeOnly = true
	// Don't update destination mod-time if files identical.
	fs.Config.NoUpdateModTime = true
	// Set proper agent for backend clients.
	fs.Config.UserAgent = UserAgent()
	// How many times to retry low level operations like copy file.
	fs.Config.LowLevelRetries = 5
	// How many stat groups to keep in memory.
	fs.Config.MaxStatsGroups = 1000
}

// UserAgent returns string value that can be used as identifier in client
// calls to the service providers.
func UserAgent() string {
	return fmt.Sprintf("Scylla Manager Agent %s", mermaid.Version())
}
