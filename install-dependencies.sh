#!/usr/bin/env bash
#
# Copyright (C) 2017 ScyllaDB
#

set -eu -o pipefail

FEDORA_PKGS="jq make moreutils sshpass rpm-build"
UBUNTU_PKGS="jq make moreutils sshpass"

GO_PKGS="
golangci-lint       https://github.com/golangci/golangci-lint/releases/download/v1.24.0/golangci-lint-1.24.0-linux-amd64.tar.gz \
sup                 https://github.com/pressly/sup/releases/download/v0.5.3/sup-linux64 \
swagger             https://github.com/go-swagger/go-swagger/releases/download/v0.25.0/swagger_linux_amd64 \
license-detector    https://github.com/src-d/go-license-detector/releases/download/2.0.2/license-detector.linux_amd64.gz \
mockgen             github.com/golang/mock/mockgen \
stress              golang.org/x/tools/cmd/stress"

source ./env
mkdir -p ${LOCAL_BIN}

echo "==> Installing system packages"
DISTRO=$(cat /etc/os-release | grep '^ID=' | cut -d= -f2)
case ${DISTRO} in
    "fedora")
        sudo dnf install ${FEDORA_PKGS}
        ;;
    "ubuntu")
        echo "> Updating package information from configured sources"
        sudo apt-get update
        echo "> Installing required system packages"
        sudo apt-get install ${UBUNTU_PKGS}
        ;;
    *)
        echo "Your OS ${DISTRO} is not supported, conciser switching to Fedora"
        exit 1
esac

echo "==> Installing cqlsh from pip"
python2.7 -m pip install cqlsh

echo "==> Cleaning ${LOCAL_BIN}"
rm -f "${LOCAL_BIN}"/*

echo "==> Installing Go packages at ${LOCAL_BIN}"

function download() {
    case $2 in
        *.tar.gz)
            ;&
        *.tgz)
            curl -sSq -L $2 | tar zxf - --strip 1 -C ${LOCAL_BIN} --wildcards "*/$1"
            ;;
        *.gz)
            curl -sSq -L $2 | gzip -d - > "${LOCAL_BIN}/$1"
            ;;
        *)
            curl -sSq -L $2 -o "${LOCAL_BIN}/$1"
            ;;
    esac
    chmod u+x "${LOCAL_BIN}/$1"
}

function install_from_vendor() {
    GO111MODULE=on go build -mod=vendor -o "${LOCAL_BIN}/$1" ${2}
}

function install() {
    echo "$1 $2"
    if [[ $2 =~ http* ]]; then
        download $1 $2
    else
        install_from_vendor $1 $2
    fi
}

pkgs=($(echo "${GO_PKGS}" | sed 's/\s+/\n/g'))
for i in "${!pkgs[@]}"; do
    if [[ $(($i % 2)) == 0 ]]; then
        install ${pkgs[$i]} ${pkgs[$((i+1))]}
    fi
done

echo "==> Complete!"
