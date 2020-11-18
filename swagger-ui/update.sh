#!/usr/bin/env bash
#
# Copyright (C) 2017 ScyllaDB
#

set -eu -o pipefail

VERSION="3.36.2"
URL="https://github.com/swagger-api/swagger-ui/archive/v${VERSION}.tar.gz"

echo "==> Cleanup"
rm -Rf dist swagger-ui.tar.gz

echo "==> Download swagger-ui ${VERSION}"
curl -sSq -L ${URL} | tar zxf - --strip 1 --no-wildcards "swagger-ui-${VERSION}/dist"

echo "==> Deploy scylla-manager.json"
cp ../pkg/managerclient/scylla-manager.json dist
sed -i 's/url: ".*",/url: "scylla-manager.json",/' dist/index.html

echo "==> Complete!"
