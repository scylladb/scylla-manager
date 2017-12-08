#!/usr/bin/env bash

set -e

source ./.env
envsubst < ./docker/Dockerfile.in > ./docker/Dockerfile
docker build -t scylladb/scylla-ssh:${SCYLLA_VERSION} ./docker
