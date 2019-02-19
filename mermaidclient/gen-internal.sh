#!/usr/bin/env bash
#
# Copyright 2018 ScyllaDB
#

set -eu -o pipefail

rm -rf internal/client internal/models
swagger generate client -A mermaid -T internal/swagger/template -f mermaid-api.json -t ./internal
