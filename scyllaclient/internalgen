#!/usr/bin/env bash
#
# Copyright 2018 ScyllaDB
#

set -eu -o pipefail

rm -rf internal/scylla/client internal/scylla/models
swagger generate client -A scylla -f scylla.json -t ./internal/scylla

rm -rf internal/rclone/client internal/rclone/models
swagger generate client -A rclone -f rclone.json -t ./internal/rclone