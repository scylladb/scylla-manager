#!/usr/bin/env bash
#
# Copyright (C) 2017 ScyllaDB
#

set -eu -o pipefail

cp ../dist/etc/scylla-manager/scylla-manager.yaml source/config
cp ../dist/etc/scylla-manager-agent/scylla-manager-agent.yaml source/config
