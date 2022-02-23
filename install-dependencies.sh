#!/usr/bin/env bash
#
# Copyright (C) 2017 ScyllaDB
#

set -eu -o pipefail

LINUX_PKGS="docker-compose jq make moreutils openssl"

GO_PKGS="
golangci-lint       github.com/golangci/golangci-lint/cmd/golangci-lint
goreleaser          github.com/goreleaser/goreleaser
license-detector    github.com/go-enry/go-license-detector/v4/cmd/license-detector
mockgen             github.com/golang/mock/mockgen
schemagen           github.com/scylladb/gocqlx/v2/cmd/schemagen
stress              golang.org/x/tools/cmd/stress
sup                 github.com/pressly/sup/cmd/sup
swagger             github.com/go-swagger/go-swagger/cmd/swagger
yq                  github.com/mikefarah/yq/v3/cmd
"

source ./env
mkdir -p ${LOCAL_BIN}

if [ -f /etc/os-release ]; then
  echo "==> Installing system packages"
  DISTRO=$(cat /etc/os-release | grep '^ID=' | cut -d= -f2)||:
  case ${DISTRO} in
      "fedora")
          sudo dnf install ${LINUX_PKGS}
          ;;
      "ubuntu")
          echo "> Updating package information from configured sources"
          sudo apt-get update
          echo "> Installing required system packages"
          sudo apt-get install ${LINUX_PKGS}
          ;;
      *)
          echo "Your OS ${DISTRO} is not supported, conciser switching to Fedora"
          ;;
  esac
fi

echo "==> Cleaning ${LOCAL_BIN}"
rm -f "${LOCAL_BIN}"/*

echo "==> Installing Go packages at ${LOCAL_BIN}"

function install() {
    echo "$1 ($2)"

    pushd mod > /dev/null
    go build -mod=readonly -o "${LOCAL_BIN}/$1" ${2}
    popd > /dev/null
}

pkgs=($(echo "${GO_PKGS}" | sed 's/\s+/\n/g'))
for i in "${!pkgs[@]}"; do
    if [[ $(($i % 2)) == 0 ]]; then
        install ${pkgs[$i]} ${pkgs[$((i+1))]}
    fi
done

echo "==> Complete!"
