#!/usr/bin/env bash
#
# Copyright 2018 ScyllaDB
#

set -eu -o pipefail

rm -rf internal/scylla/client internal/scylla/models
swagger generate client -A scylla -f scylla.json -t ./internal/scylla

rm -rf internal/scylla_v2/client internal/scylla_v2/models
swagger generate client -A scylla2 -f scylla_v2.json -t ./internal/scylla_v2

rm -rf internal/rclone/client internal/rclone/models
swagger generate client -A rclone -f rclone.json -t ./internal/rclone