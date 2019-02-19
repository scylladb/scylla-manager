#!/usr/bin/env bash
#
# Copyright 2018 ScyllaDB
#

set -eu -o pipefail

rm -rf internal/client internal/models
swagger generate client -A scylla -f scylla-api.json -t ./internal
