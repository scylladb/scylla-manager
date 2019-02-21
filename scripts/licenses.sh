#!/usr/bin/env bash

set -eu -o pipefail

for pkg in `grep '  name =' ../Gopkg.lock | cut -d '"' -f 2 | sort -f` ; do
    (echo $pkg >&2)

    l=`license-detector -f json "../vendor/$pkg" | jq -r '.[0].matches[0].license'`
    echo Project $pkg licensed under $l license.
    echo
    cat ../vendor/$pkg/LICENSE* ||:
    echo
    echo
done
