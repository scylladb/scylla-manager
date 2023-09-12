#!/usr/bin/env bash
#
# Copyright (C) 2017 ScyllaDB
#

set -eu -o pipefail

LINUX_PKGS="docker-compose jq make moreutils openssl"

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

export GOBIN=${LOCAL_BIN}
go install github.com/golangci/golangci-lint/cmd/golangci-lint@v1.54.2
go install github.com/goreleaser/goreleaser@v1.14.1
go install github.com/go-enry/go-license-detector/v4/cmd/license-detector@latest
go install github.com/golang/mock/mockgen@v1.6.0
go install github.com/scylladb/gocqlx/v2/cmd/schemagen@v2.6.0
go install golang.org/x/tools/cmd/stress@v0.2.0
go install github.com/pressly/sup/cmd/sup@v0.5.3
go install github.com/go-swagger/go-swagger/cmd/swagger@v0.25.0
go install github.com/mikefarah/yq/v3@v3.0.0-20201202084205-8846255d1c37

echo "==> Complete!"
