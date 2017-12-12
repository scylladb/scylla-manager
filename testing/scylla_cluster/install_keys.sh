#!/usr/bin/env bash

set -e

SSH_OPTS="-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null"

echo "==> Generating keys"
[ -f "id_rsa" ] || ssh-keygen -t rsa -b 2048 -N "" -f "id_rsa" > /dev/null && chmod 0400 id_rsa

echo "==> Installing keys"
for name in `docker ps -f name=mermaid_dc* --format {{.Names}}`; do
	ip=`docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' ${name}`
	echo " ${ip}"
	sshpass -p "test" ssh-copy-id -i id_rsa -f ${SSH_OPTS} scylla-mgmt@${ip} &> /dev/null
done
