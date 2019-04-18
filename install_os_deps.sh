#!/usr/bin/env bash

set -u -o pipefail

FEDORA_PKGS="jq make moreutils sshpass pcre-tools python2   python2-pip rpm-build"
UBUNTU_PKGS="jq make moreutils sshpass pcregrep   python2.7 python-pip"

PYTHON_PKGS="cqlsh docker-compose"

echo "> Installing system packages"
DISTRO=` cat /etc/os-release | grep '^ID=' | cut -d= -f2`
case ${DISTRO} in
    "fedora")
        sudo dnf install ${FEDORA_PKGS}
        ;;
    "ubuntu")
        echo "> Installing required system packages"
        sudo apt-get install ${UBUNTU_PKGS}
        ;;
    *)
        echo "Your OS ${DISTRO} is not supported, conciser switching to Fedora"
        exit 1
esac

echo "> Installing python packages"
pip install --user ${PYTHON_PKGS}
