#!/usr/bin/env bash

set -e
rm -Rvf internal
mkdir internal
swagger generate client -A mermaid -T ../swagger/template -f ../swagger/restapi.json -t ./internal
