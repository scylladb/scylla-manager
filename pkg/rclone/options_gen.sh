#!/usr/bin/env bash
#
# Copyright (C) 2017 ScyllaDB
#

set -eu -o pipefail

go run $(git rev-parse --show-toplevel)/scripts/rclone_options.go > options_gen.go
