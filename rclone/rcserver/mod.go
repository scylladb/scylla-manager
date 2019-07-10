// Copyright (C) 2017 ScyllaDB

package rcserver

import (
	// Needed for vendoring and tests
	_ "github.com/ncw/rclone/backend/local"
	_ "github.com/ncw/rclone/backend/s3"
	_ "github.com/ncw/rclone/fs/accounting"
	_ "github.com/ncw/rclone/fs/operations"
	_ "github.com/ncw/rclone/fs/sync"
)
