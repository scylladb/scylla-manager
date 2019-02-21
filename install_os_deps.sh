#!/usr/bin/env bash

set -u -o pipefail

FEDORA=$(command -v dnf)
UBUNTU="$(command -v apt-get)"

set -e

if [ -n "$FEDORA" ]; then
    sudo dnf install jq make sshpass python2 python2-pip
    # For building cross platform packages.
    sudo dnf install createrepo rpm-build moreutils
elif [ -n "$UBUNTU" ]; then
    sudo apt-get install jq make sshpass python2.7 python-pip
else
    echo "Unsupported OS."
    echo "Only Fedora and Ubuntu are supported atm."
    exit 1
fi

# Install Python specific tools
if ! command -v docker-compose; then
    pip install --user docker-compose
fi
if ! command -v cqlsh; then
    pip install --user cqlsh
fi
