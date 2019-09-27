// Copyright (C) 2017 ScyllaDB

package rcserver

import (
	// Needed for triggering global registrations in rclone.
	_ "github.com/rclone/rclone/backend/local"
	_ "github.com/rclone/rclone/backend/s3"
	_ "github.com/rclone/rclone/fs/accounting"
	_ "github.com/rclone/rclone/fs/operations"
	_ "github.com/rclone/rclone/fs/rc/jobs"
	_ "github.com/rclone/rclone/fs/sync"
	_ "github.com/scylladb/mermaid/rclone/backend/data"
)
