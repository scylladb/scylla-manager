#!/usr/bin/env bash
#
# Copyright (C) 2017 ScyllaDB
#

set -eu -o pipefail

GH_PAGES_DIR="${1}"

rm -Rf "${GH_PAGES_DIR}"
mkdir "${GH_PAGES_DIR}"
cd "${GH_PAGES_DIR}"

git init
git config --local user.email "action@scylladb.com"
git config --local user.name "GitHub Action"
git remote add origin "https://x-access-token:${GITHUB_TOKEN}@github.com/${GITHUB_REPOSITORY}.git"
git fetch origin gh-pages
git checkout gh-pages
