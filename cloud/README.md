# Cloud testing tools

This package provides tools to work with cloud lab and other scylla RHEL installations.
It is built with [sup - Simple Deployment Tool](https://pressly.github.io/sup/).

# Getting Started

1. Create `cloud-lab` aws profile by appending `aws/profile` file to `~/.aws/config`.
1. Go to https://front.lab.dbaas.scyop.net/ and create a new AWS cluster in `eu-central-1` region.
   If you create the cluster in a different region please update the region in the `cloud-lab` profile.
1. Get the cluster ID from URL.
1. Run the AWS setup script `cd aws; ./setup.sh CLUSTER_ID`
1. Get [Lab support pem file](https://github.com/scylladb/siren-devops/blob/master/cluster/maintenance/access/lab/support.pem) and save it as `~/.ssh/id_scylla-lab-support.pem`, the `id_` prefix is mandatory for `sup` to automatically load it

**Working with ssh-agent**

If you dislike the `id_` prefix or would like to keep the key elsewhere you can work with `ssh-agent` to load the key.

Example:

```bash
eval `ssh-agent`
Agent pid 16210
ssh-add ~/.ssh/scylla-lab-support.pem 
Identity added: /home/michal/.ssh/scylla-lab-support.pem (/home/michal/.ssh/scylla-lab-support.pem)
```

*WARNING* do not forget to kill the agent when you are done.

# Backup and S3 buckets

The AWS setup script assigns nodes a role that gives them access to all `manager-test*` s3 buckets.
You may create a new S3 bucket or use `s3:manager-test-demo1`.

# Scylla Manager update from a distribution

1. Run `sup server "stop service"`
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
  