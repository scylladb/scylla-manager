#!/bin/bash
#
# Copyright (C) 2017 ScyllaDB
#

set -eu -o pipefail

source .env

function mc() {
  docker run -it --rm --network=host -v $(pwd)/minio/mc:/root/.mc/ -v $(pwd)/minio/user-policy.json:/user-policy.json  minio/mc:"$MINIO_MC_VERSION" "$@"
}

mc config host add minio-test "$MINIO_ENDPOINT" "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD"
mc admin policy add minio-test user /user-policy.json
mc admin user add minio-test "$MINIO_USER_ACCESS_KEY" "$MINIO_USER_SECRET_KEY"
mc admin policy set minio-test user user="$MINIO_USER_ACCESS_KEY"