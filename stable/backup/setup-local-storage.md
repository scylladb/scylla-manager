# Setup Local Storage

> * [Prerequisites](#prerequisites)
> * [Create a bucket](#create-a-bucket)
> * [Configure the agent](#configure-the-agent)
> * [Troubleshoot connectivity](#troubleshoot-connectivity)
> * [NFS setup example](#nfs-setup-example)
>   * [Server side](#server-side)
>   * [Client side](#client-side)

Local storage allows you to store backups on a network-mounted filesystem path accessible by the ScyllaDB Manager Agent.
Each backup location’s bucket name becomes a subdirectory under the configured root path.

#### WARNING
Native backup and native restore are **not available** with the local storage provider.
Only rclone-based backup and restore are supported.

## Prerequisites

All nodes within the same datacenter **must have access to the same shared filesystem**.

A common way to achieve this is by using NFS (Network File System).
The [NFS setup example]() describes a simple NFS setup.

## Create a bucket

With local storage, a bucket is simply a subdirectory under the configured root path.
You need to create it before backing up data, the same as you would create a bucket with any other storage provider.

For example, if shared filesystem path is `/mnt/nfs` and the desired bucket name is `manager`:

```none
sudo mkdir /mnt/nfs/manager
```

Make sure the directory is writable by the ScyllaDB Manager Agent.

## Configure the agent

This procedure needs to be repeated for each ScyllaDB node.

**Procedure**

Edit the `/etc/scylla-manager-agent/scylla-manager-agent.yaml`

1. Uncomment the `localstorage:` line, for parameters note the two spaces in front, it’s a yaml file.
2. Uncomment and set `path` to the absolute path of the shared filesystem mount point (e.g. `/mnt/nfs`).
   ```yaml
   localstorage:
     path: /mnt/nfs
   ```
3. Validate that the agent has access to the backup location.
   If there is no response, the location is accessible. If not, you will see an error.
   ```none
   scylla-manager-agent check-location --location localstorage:<bucket>
   ```
4. Restart ScyllaDB Manager Agent service.
   ```none
   sudo systemctl restart scylla-manager-agent
   ```

## Troubleshoot connectivity

To troubleshoot ScyllaDB node to local storage connectivity issues you can run:

```none
scylla-manager-agent check-location --debug --location localstorage:<bucket>
```

## NFS setup example

### Server side

1. Install the NFS server package.

   Ubuntu
   ```console
   sudo apt install nfs-kernel-server
   ```
2. Prepare a shared directory. Format and mount a dedicated block device:
   ```none
   sudo mkfs -t ext4 /dev/<device>
   sudo mkdir -p /mnt/nfs
   sudo mount /dev/<device> /mnt/nfs
   ```

   Replace `<device>` with the name of your block device (e.g. `sdb`, `nvme1n1`).
3. Export the shared directory by adding the following line to `/etc/exports`:
   ```none
   /mnt/nfs NFS_CLIENT_SUBNET(rw,sync,all_squash,no_subtree_check)
   ```

   Replace `NFS_CLIENT_SUBNET` with the subnet of your ScyllaDB nodes (e.g., `172.31.0.0/16`).
4. Apply the export configuration and restart the NFS server.
   ```none
   sudo exportfs -ra
   sudo systemctl restart nfs-kernel-server
   ```

#### NOTE
The `all_squash` option maps all client UIDs/GIDs to the anonymous user, which is a simple way of ensuring that
all agents have read/write access to backup data from all nodes within the same datacenter.
Evaluate whether this option fits your security requirements before using it in production.

### Client side

This procedure needs to be repeated for **each ScyllaDB node**.

1. Install the NFS client package.

   Ubuntu
   ```console
   sudo apt install nfs-common
   ```
2. Create a mount point and mount the NFS share.
   ```none
   sudo mkdir -p /mnt/nfs
   sudo mount NFS_SERVER_IP_ADDRESS:/mnt/nfs /mnt/nfs
   ```

   Replace `NFS_SERVER_IP_ADDRESS` with the IP address of your NFS server.
3. To make the mount persistent across reboots, add the following line to `/etc/fstab`:
   ```none
   NFS_SERVER_IP_ADDRESS:/mnt/nfs  /mnt/nfs  nfs  defaults,nofail  0  0
   ```
