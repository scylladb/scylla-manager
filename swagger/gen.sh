#!/usr/bin/env bash
set -e

if [ -z ${SCYLLA_API_DOC} ]; then
    echo "set SCYLLA_API_DOC env variable pointing to Scylla api/api-doc folder"
    exit 1
fi

if [ -z ${APIMATIC_USER} ]; then
    echo "set APIMATIC_USER env variable to a valid user:password for apimatic.io"
    exit 1
fi

jq -M -s 'reduce .[] as $x (.[0]; .apis += $x.apis? | .models += $x.models) | .resourcePath="/"' ${SCYLLA_API_DOC}/*.json \
| sed -e 's/#\/utils\///g' \
| sed -e 's/"bool"/"boolean"/g' \
| curl -f -XPOST -d @- --user ${APIMATIC_USER} --silent --output "scylla-api.json" https://apimatic.io/api/transform?format=swagger20
