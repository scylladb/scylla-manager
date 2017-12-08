#!/usr/bin/env bash

set -e
rm -Rvf internal
mkdir internal
swagger generate client -A scylla -f ../swagger/scylla.json -t ./internal
