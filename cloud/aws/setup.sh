#!/bin/bash
#
# Copyright (C) 2017 ScyllaDB
#

set -eu -o pipefail

CLUSTER_ID="${1}"
KEEP_HOURS="${KEEP_HOURS:-48}"
IAM_ROLE="Manager_Test_Role"

AWS="aws --profile cloud-lab"

function instances {
    ${AWS} ec2 describe-instances --filter "Name=tag:Name,Values=*${CLUSTER_ID}*" \
    | jq -r '.Reservations[].Instances[].InstanceId'
}

function sg {
    ${AWS} ec2 describe-instances --filter "Name=tag:Name,Values=*${CLUSTER_ID}*" \
    | jq -r '.Reservations[].Instances[].NetworkInterfaces[].Groups[] | select(.GroupName | contains ("Admin")) | .GroupId' \
    | head -n 1
}

function my_cidr {
    echo "$(curl -s ifconfig.me)/32"
}

function node_ips {
    ${AWS} ec2 describe-instances --filter "Name=tag:Name,Values=*${CLUSTER_ID}*node*" \
    | jq -r '.Reservations[].Instances[].PublicIpAddress'
}

function manager_ip {
    ${AWS} ec2 describe-instances --filter "Name=tag:Name,Values=*${CLUSTER_ID}*Monitor*" \
    | jq -r '.Reservations[].Instances[].PublicIpAddress'
}

echo "Using cluster ${CLUSTER_ID}"

echo "Setting tag keep:${KEEP_HOURS}"
${AWS} ec2 create-tags --resources $(instances) --tags "Key=keep,Value=${KEEP_HOURS}"

echo "Generating sup network files"
node_ips | sed -e 's/^/support@/' > ../networks/agent.hosts
manager_ip | sed -e 's/^/support@/' > ../networks/server.host

echo "Assigning role ${IAM_ROLE}"
for i in $(instances); do
    ${AWS} ec2 associate-iam-instance-profile --instance-id "${i}" --iam-instance-profile Name="${IAM_ROLE}" > /dev/null
done

GROUP_ID="$(sg)"
CIDR="$(my_cidr)"
for p in 22 5112 9042 10001; do
    echo "Opening port ${p}"
    ${AWS} ec2 authorize-security-group-ingress --group-id "${GROUP_ID}" --protocol tcp --port ${p} --cidr "${CIDR}"
done
