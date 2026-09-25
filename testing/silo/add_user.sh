#!/bin/bash
#
# Copyright (C) 2017 ScyllaDB
#

set -eu -o pipefail

source .env

function mc() {
  docker run -i --rm --network=host -v $(pwd)/silo/mc:/root/.mc/ -v $(pwd)/silo/user-policy.json:/user-policy.json  docker.io/pgsty/mc:"$SILO_MC_VERSION" "$@"
}

mc --insecure alias set silo-test "$SILO_ENDPOINT" "$SILO_ROOT_USER" "$SILO_ROOT_PASSWORD"
mc --insecure admin policy create silo-test user /user-policy.json || mc --insecure admin policy update silo-test user /user-policy.json
mc --insecure admin user add silo-test "$SILO_USER_ACCESS_KEY" "$SILO_USER_SECRET_KEY" || true
mc --insecure admin policy attach silo-test user --user "$SILO_USER_ACCESS_KEY"
