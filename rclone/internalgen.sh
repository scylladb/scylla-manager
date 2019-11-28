#!/usr/bin/env bash
#
# Copyright (C) 2017 ScyllaDB
#

rm -f internal/rclone_supported_calls.go
jq '.paths | [keys[] | select(. | startswith("/rclone")) | sub("^/rclone/"; "")]' ../scyllaclient/agent.json | \
  go run internal/templates/jsontemplate.go internal/templates/rclone_supported_calls.gotmpl > \
  internal/rclone_supported_calls.go