// Copyright (C) 2017 ScyllaDB

package scyllaclient

func SplitRemotePath(remotePath string) (string, string, error) {
	return rcloneSplitRemotePath(remotePath)
}
