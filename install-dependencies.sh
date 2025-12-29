#!/usr/bin/env bash
#
# Copyright (C) 2017 ScyllaDB
#

set -eu -o pipefail

LINUX_PKGS="jq make moreutils openssl"

source ./env
mkdir -p ${LOCAL_BIN}

# Detect OS
OS_TYPE="unknown"
if [[ "$OSTYPE" == "darwin"* ]]; then
  OS_TYPE="macos"
elif [ -f /etc/os-release ]; then
  OS_TYPE="linux"
fi

# Install system packages
if [ "$OS_TYPE" = "linux" ]; then
  echo "==> Installing system packages (Linux)"
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
          echo "Your OS ${DISTRO} is not supported, consider switching to Fedora"
          ;;
  esac
elif [ "$OS_TYPE" = "macos" ]; then
  echo "==> Skipping system packages installation on macOS"
  echo "    If needed, install manually: brew install jq moreutils"
fi

echo "==> Cleaning ${LOCAL_BIN}"
rm -f "${LOCAL_BIN}"/*

echo "==> Installing Go packages at ${LOCAL_BIN}"

# Allow Go to automatically download and use the correct toolchain
export GOTOOLCHAIN=auto
export GOBIN=${LOCAL_BIN}

# Install packages one by one, continuing on error
go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.6.2 || echo "Warning: Failed to install golangci-lint"
go install github.com/goreleaser/goreleaser@v1.14.1 || echo "Warning: Failed to install goreleaser"
go install github.com/go-enry/go-license-detector/v4/cmd/license-detector@latest || echo "Warning: Failed to install license-detector"
go install github.com/golang/mock/mockgen@v1.6.0 || echo "Warning: Failed to install mockgen"
go install github.com/scylladb/gocqlx/v2/cmd/schemagen@v2.6.0 || echo "Warning: Failed to install schemagen"
go install golang.org/x/tools/cmd/stress@v0.2.0 || echo "Warning: Failed to install stress"
go install github.com/pressly/sup/cmd/sup@v0.5.3 || echo "Warning: Failed to install sup"
go install github.com/go-swagger/go-swagger/cmd/swagger@v0.25.0 || echo "Warning: Failed to install swagger"
go install github.com/mikefarah/yq/v4@latest || echo "Warning: Failed to install yq"

echo "==> Complete!"
