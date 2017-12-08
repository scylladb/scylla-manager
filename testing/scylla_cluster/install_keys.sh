#!/usr/bin/env bash

set -e

for name in `docker ps -f name=mermaid_dc* --format {{.Names}}`; do
	sshpass -p "test" ssh-copy-id -f -oStrictHostKeyChecking=no scylla-mgmt@`docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' ${name}` &>/dev/null;
done
