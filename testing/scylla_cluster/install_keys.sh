#!/usr/bin/env bash

set -e

echo "==> Generating keys"
[ -f "scylla_mgmt" ] || ssh-keygen -t rsa -b 2048 -N "" -f "scylla_mgmt"

pem=`cat "scylla_mgmt.pub"`

echo "==> Installing keys"
for name in `docker ps -f name=mermaid_dc* --format {{.Names}}`; do
	ip=`docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' ${name}`
	echo " ${ip}"
	sshpass -p "test" ssh-copy-id -f -o StrictHostKeyChecking=no -o UserKnownHostsFile=scylla_mgmt_known_hosts scylla-mgmt@${ip} &> /dev/null
done

echo "==> Linking files"
ln -f scylla_mgmt scylla_mgmt.pub scylla_mgmt_known_hosts ~/.ssh/

echo "try: ssh -i ~/.ssh/scylla_mgmt -o UserKnownHostsFile=~/.ssh/scylla_mgmt_known_hosts scylla-mgmt@172.16.1.10"
