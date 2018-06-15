#!/usr/bin/env bash

set -e

SSH_OPTS="-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null"
KEY_FILE=cluster.pem

[ -f "${KEY_FILE}" ] || ssh-keygen -t rsa -b 2048 -N "" -f "${KEY_FILE}" > /dev/null && chmod 0400 "${KEY_FILE}"

for name in `docker ps -f name=mermaid_dc --format {{.Names}}`; do
	ip=`docker inspect -f '{{with index .NetworkSettings.Networks "mermaid_public"}}{{.IPAddress}}{{end}}' ${name}`
	echo "${ip}"
	sshpass -p "test" ssh-copy-id -i "${KEY_FILE}" -f ${SSH_OPTS} scylla-manager@${ip} &> /dev/null
done
