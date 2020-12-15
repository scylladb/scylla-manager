#!/usr/bin/env bash
#
# Copyright (C) 2017 ScyllaDB
#

set -eu -o pipefail

GH_PAGES_DIR="${1}"
MSG="${2}"

cd "${GH_PAGES_DIR}"

git add .
git commit -m "${MSG}" || :
git push origin gh-pages --force
