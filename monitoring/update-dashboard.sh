#!/usr/bin/env bash
#
# Copyright (C) 2026 ScyllaDB
#
# Builds the Scylla Manager dashboard of the ScyllaDB Monitoring stack
# (scylla-monitoring) at a pinned commit and saves it as
# monitoring/scylla-manager.json, the file the development environment's
# Grafana loads.
#
# The dashboard is developed in scylla-monitoring, as
# grafana/scylla-manager.template.json. To pick up a change made there, bump
# SCYLLA_MONITORING_REF below, run this script and commit both.
#
# The build runs in a container, so only docker is needed.
#
# usage: monitoring/update-dashboard.sh [scylla-monitoring commit]

set -euo pipefail

# scylladb/scylla-monitoring#2982: "scylla-manager: show task and health check status".
SCYLLA_MONITORING_REF="${1:-50223512689a0552e1bf5b4bbc2296a4edb37ff2}"
# Scylla Manager major version the dashboard is built for.
MANAGER_VERSION=3

out_dir="$(cd "$(dirname "$0")" && pwd)"

docker run --rm \
	-v "${out_dir}:/out" \
	-e REF="${SCYLLA_MONITORING_REF}" \
	-e MANAGER_VERSION="${MANAGER_VERSION}" \
	-e OUT_UID="$(id -u)" \
	-e OUT_GID="$(id -g)" \
	-e DEBIAN_FRONTEND=noninteractive \
	python:3.12-slim bash -euo pipefail -c '
		apt-get -qq update >/dev/null
		apt-get -qq install -y git >/dev/null 2>&1
		pip install -q pyyaml 2>/dev/null
		git clone -q https://github.com/scylladb/scylla-monitoring /src
		cd /src
		git checkout -q "${REF}"
		./generate-dashboards.sh -M "${MANAGER_VERSION}" -F -P no-version-check -P no-your-pannels >/tmp/generate.log 2>&1 || {
			tail -20 /tmp/generate.log
			exit 1
		}
		f="grafana/build/manager_${MANAGER_VERSION}/scylla-manager.${MANAGER_VERSION}.json"
		cp "$f" /out/scylla-manager.json
		chown "${OUT_UID}:${OUT_GID}" /out/scylla-manager.json
	'

echo "monitoring/scylla-manager.json built from scylladb/scylla-monitoring@${SCYLLA_MONITORING_REF}"
