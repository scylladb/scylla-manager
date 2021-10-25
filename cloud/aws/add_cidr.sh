#!/bin/bash
#
# Copyright (C) 2017 ScyllaDB
#

CLUSTER_ID="${1}"
CIDR="${2}"

AWS="aws --profile cloud-lab"

function sg {
    ${AWS} ec2 describe-instances --filter "Name=tag:Name,Values=*${CLUSTER_ID}*" \
    | jq -r '.Reservations[].Instances[].NetworkInterfaces[].Groups[] | select(.GroupName | contains ("Admin")) | .GroupId' \
    | head -n 1
}

GROUP_ID="$(sg)"
for p in 22 5112 9042 10001; do
    echo "Opening port ${p}"
    ${AWS} ec2 authorize-security-group-ingress --group-id "${GROUP_ID}" --protocol tcp --port ${p} --cidr "${CIDR}"
done
