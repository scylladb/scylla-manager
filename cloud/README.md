# Cloud testing tools

This package provides tools to work with cloud lab and other scylla RHEL installations.
It is built with [sup - Simple Deployment Tool](https://pressly.github.io/sup/).

# Getting Started

1. Go to https://front.lab.dbaas.scyop.net/ and create a cluster
1. Go to AWS console for Cloud Lab:
  - For every VM in your cluster add tags Key: `keep` Value: `alive` to prevent from garbage collecting your cluster after 2h
  - Take any VM in your cluster and edit the first Security Group by enabling SSH access from your machine
1. Get [Lab support pem file](https://github.com/scylladb/siren-devops/blob/master/cluster/maintenance/access/lab/support.pem) and save it as `~/.ssh/id_scylla-lab-support.pem`, the `id_` prefix is mandatory for `sup` to automatically load it

## Working with ssh-agent

If you dislike the `id_` prefix or would like to keep the key elsewhere you can work with `ssh-agent` to load the key.

Example:

```bash
eval `ssh-agent`
Agent pid 16210
ssh-add ~/.ssh/scylla-lab-support.pem 
Identity added: /home/michal/.ssh/scylla-lab-support.pem (/home/michal/.ssh/scylla-lab-support.pem)
```

*WARNING* do not forget to kill the agent when you are done.

# Scylla Manager Installation

When you have a cluster up and running and you can SSH to any node, it's time to cook the `sup`.

## Register nodes in networks

1. Put all IPs of node VMs (name like `Scylla-Cloud-...-Node-X`) in `networks/agent.hosts` each in a new line, don't forget to add `support@` user prefix to IP
1. Put IP of monitor node (name like `Scylla-Cloud-...-Monitor`) in `networks/server.host`, don't forget to add `support@` user prefix to IP.

## Let the knife do the sup

1. Run `sup agent install start`
1. Run `sup server install`
1. Go to server edit config file `/etc/scylla-manager/scylla-manager.yaml` db section to use the created cluster (FIXME waiting for a volunteer to fix that step)
1. Run `sup server start`

# Updating binaries to your local dev builds

1. Build agent or server binaries with `make dev-agent` or `make dev-server` in the root of the project
1. Run `sup agent update` or `sup server update` this will upload dev binary and restart service

# Changing sstable file names for testing purge

In order to activate purge of the backup files should no longer be needed once the retention policy is activated.
To simulate file change you can run the script that will change sstable file names without corrupting the database.

```bash
rename-sstable-files.sh <directory>
```  

Script will recursively scan provided directory and rename any sstable files that it finds but ignoring other files.
Because rename adds content to the file name script can be run only several times until the Scylla imposed limits are reached.
This depends on the initial file name structure.
  