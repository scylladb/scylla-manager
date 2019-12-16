#!/usr/bin/env bash
#
# Copyright (C) 2017 ScyllaDB
#

set -eu -o pipefail

if [[ ! -f "/var/lib/scylla-manager/scylla_manager.crt" || ! -f "/var/lib/scylla-manager/scylla_manager.key" ]]; then
   /sbin/scyllamgr_ssl_cert_gen
fi
exec /usr/bin/scylla-manager-agent $@
