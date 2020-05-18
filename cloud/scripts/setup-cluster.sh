#!/bin/bash

# Setup Scylla Cloud lab cluster for testing after it's created.
# 
# It sequentially does following:
#  - Creates ssh config file under ~/.ssh path (use Include directive in ~/.ssh/config to include these kind of files)
#  - Adds keep=48 tags to the instances
#  - Assigns IAM role to each instance that gives it access to the buckets that have prefix "manager-test-"
#  - Opens ports 9042, 10001, 22, 3000, 9090 for local machine and monitoring node
#
# Script is idempotent and it may be run multiple times for the same cluster.
# If some of the steps is already executed it will print errors and continue with the next one.
# 
# Script accepts single lab cluster id as parameter.

set -xu -o pipefail

if [[ $# -eq 0 ]]; then
    echo "No arguments supplied"
    exit 1
fi

if [[ $1 == "" ]]; then
    echo "Empty argument supplied"
    exit 1
fi

CLUSTER_ID="$1"
AWSPROFILE="${AWSPROFILE:---profile scylab}"
TMPDIR="${TMPDIR:-/tmp/setup-cloud}"
METADATA_FILE="$TMPDIR/metadata-$CLUSTER_ID.txt"
KEEP_HOURS="${KEEP_HOURS:-48}"
IAM_ROLE="${IAM_ROLE:-Manager_Test_Role}"
CLOUD_IDENTITY="${CLOUD_IDENTITY:-$HOME/.ssh/scylla-cloud-support.pem}"

mkdir -p "$TMPDIR"

# Get instance metadata
aws $AWSPROFILE ec2 describe-instances --filter 'Name=tag:Name,Values=*'"$CLUSTER_ID"'*' > "$METADATA_FILE"

# Add ssh config
NODE1=$(cat $METADATA_FILE | grep "INSTANCES" | grep "Node0" | awk '{print $15}')
NODE2=$(cat $METADATA_FILE | grep "INSTANCES" | grep "Node1" | awk '{print $15}')
NODE3=$(cat $METADATA_FILE | grep "INSTANCES" | grep "Node2" | awk '{print $15}')
MONITOR=$(cat $METADATA_FILE | grep "INSTANCES" | grep "Moni" | awk '{print $15}')

cat > "$HOME/.ssh/config_scylab_$CLUSTER_ID" <<EOM
Host node1
  Hostname $NODE1
  User support
  IdentityFile $CLOUD_IDENTITY
Host node2
  Hostname $NODE2
  User support
  IdentityFile $CLOUD_IDENTITY
Host node3
  Hostname $NODE3
  User support
  IdentityFile $CLOUD_IDENTITY
Host monitor
  Hostname $MONITOR
  User support
  IdentityFile $CLOUD_IDENTITY
EOM
echo "Added ssh config"

INSTANCES=$(cat $METADATA_FILE | grep "INSTANCES" | awk '{print $9}')

# Assign keep tags
RESOURCES=$(echo "$INSTANCES" | tr '\n' ' ')
aws $AWSPROFILE ec2 create-tags \
    --resources $RESOURCES \
    --tags "Key=keep,Value=$KEEP_HOURS"
echo "Assigned tags to $RESOURCES"

# Assign IAM role to the instances
while read -r instance; do
    association=$(aws $AWSPROFILE ec2 describe-iam-instance-profile-associations --filters 'Name=instance-id,Values='$instance | grep "assoc" | awk '{print $2}')
    if [ -z "$association" ]
    then
          aws $AWSPROFILE ec2 associate-iam-instance-profile --instance-id $instance --iam-instance-profile Name="$IAM_ROLE"
    else
          aws $AWSPROFILE ec2 replace-iam-instance-profile-association --iam-instance-profile Name="$IAM_ROLE" --association-id $association
    fi
done <<< "$INSTANCES"
echo "Assigned iam role to instances"

# Get IP of local machine
curl --silent v4.ifconfig.co > "$TMPDIR/ip.txt"
awk '{ print $0 "/32" }' < "$TMPDIR/ip.txt" > "$TMPDIR/ipnew.txt"
echo "Got ip for this machine"
export IPRANGE="$(cat $TMPDIR/ipnew.txt)"
export MONITRANGE="$MONITOR/32"

# Open ports for local machine and monitoring node
GROUPID="$(cat $METADATA_FILE | grep SGAdmin | awk '{print $2}' | head -n 1)"
aws $AWSPROFILE ec2 authorize-security-group-ingress --group-id "$GROUPID" \
 --protocol tcp --port 9042 --cidr $MONITRANGE
aws $AWSPROFILE ec2 authorize-security-group-ingress --group-id "$GROUPID" \
 --protocol tcp --port 10001 --cidr $MONITRANGE
aws $AWSPROFILE ec2 authorize-security-group-ingress --group-id "$GROUPID" \
 --protocol tcp --port 22 --cidr $IPRANGE
aws $AWSPROFILE ec2 authorize-security-group-ingress --group-id "$GROUPID" \
 --protocol tcp --port 9042 --cidr $IPRANGE
aws $AWSPROFILE ec2 authorize-security-group-ingress --group-id "$GROUPID" \
 --protocol tcp --port 3000 --cidr $IPRANGE
aws $AWSPROFILE ec2 authorize-security-group-ingress --group-id "$GROUPID" \
 --protocol tcp --port 9090 --cidr $IPRANGE
echo "Opened ports"
