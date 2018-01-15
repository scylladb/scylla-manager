#!/usr/bin/env bash

set -e

SSH_OPTS="-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null"
KEYFILE=scylla_manager.pem

echo "==> Generating keys"
[ -f "${KEYFILE}" ] || ssh-keygen -t rsa -b 2048 -N "" -f "${KEYFILE}" > /dev/null && chmod 0400 "${KEYFILE}"

echo "==> Installing keys"
for name in `docker ps -f name=mermaid_dc* --format {{.Names}}`; do
	ip=`docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' ${name}`
	echo " ${ip}"
	sshpass -p "test" ssh-copy-id -i "${KEYFILE}" -f ${SSH_OPTS} scylla-manager@${ip} &> /dev/null
done
