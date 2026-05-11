===================
Setup Local Storage
===================

.. contents::
   :depth: 2
   :local:

Local storage allows you to store backups on a network-mounted filesystem path accessible by the ScyllaDB Manager Agent.
Each backup location's bucket name becomes a subdirectory under the configured root path.

.. warning::

   Native backup and native restore are **not available** with the local storage provider.
   Only rclone-based backup and restore are supported.

Prerequisites
=============

All nodes within the same datacenter **must have access to the same shared filesystem**.

A common way to achieve this is by using NFS (Network File System).
The `NFS setup example`_ describes a simple NFS setup.

Create a bucket
===============

With local storage, a bucket is simply a subdirectory under the configured root path.
You need to create it before backing up data, the same as you would create a bucket with any other storage provider.

For example, if shared filesystem path is ``/mnt/nfs`` and the desired bucket name is ``manager``:

.. code-block:: none

   sudo mkdir /mnt/nfs/manager

Make sure the directory is writable by the ScyllaDB Manager Agent.

Configure the agent
===================

This procedure needs to be repeated for each ScyllaDB node.

**Procedure**

Edit the ``/etc/scylla-manager-agent/scylla-manager-agent.yaml``

#. Uncomment the ``localstorage:`` line, for parameters note the two spaces in front, it's a yaml file.
#. Uncomment and set ``path`` to the absolute path of the shared filesystem mount point (e.g. ``/mnt/nfs``).

   .. code-block:: yaml

      localstorage:
        path: /mnt/nfs

#. Validate that the agent has access to the backup location.
   If there is no response, the location is accessible. If not, you will see an error.

   .. code-block:: none

      scylla-manager-agent check-location --location localstorage:<bucket>

#. Restart ScyllaDB Manager Agent service.

   .. code-block:: none

      sudo systemctl restart scylla-manager-agent

Troubleshoot connectivity
=========================

To troubleshoot ScyllaDB node to local storage connectivity issues you can run:

.. code-block:: none

   scylla-manager-agent check-location --debug --location localstorage:<bucket>

NFS setup example
=================

Server side
-----------

#. Install the NFS server package.

   .. tabs::

      .. group-tab:: Ubuntu

           .. code-block:: console

              sudo apt install nfs-kernel-server

#. Prepare a shared directory. Format and mount a dedicated block device:

   .. code-block:: none

      sudo mkfs -t ext4 /dev/<device>
      sudo mkdir -p /mnt/nfs
      sudo mount /dev/<device> /mnt/nfs

   Replace ``<device>`` with the name of your block device (e.g. ``sdb``, ``nvme1n1``).

#. Export the shared directory by adding the following line to ``/etc/exports``:

   .. code-block:: none

      /mnt/nfs NFS_CLIENT_SUBNET(rw,sync,all_squash,no_subtree_check)

   Replace ``NFS_CLIENT_SUBNET`` with the subnet of your ScyllaDB nodes (e.g., ``172.31.0.0/16``).

#. Apply the export configuration and restart the NFS server.

   .. code-block:: none

      sudo exportfs -ra
      sudo systemctl restart nfs-kernel-server

.. note::

   The ``all_squash`` option maps all client UIDs/GIDs to the anonymous user, which is a simple way of ensuring that
   all agents have read/write access to backup data from all nodes within the same datacenter.
   Evaluate whether this option fits your security requirements before using it in production.

Client side
-----------

This procedure needs to be repeated for **each ScyllaDB node**.

#. Install the NFS client package.

   .. tabs::

      .. group-tab:: Ubuntu

           .. code-block:: console

              sudo apt install nfs-common

#. Create a mount point and mount the NFS share.

   .. code-block:: none

      sudo mkdir -p /mnt/nfs
      sudo mount NFS_SERVER_IP_ADDRESS:/mnt/nfs /mnt/nfs

   Replace ``NFS_SERVER_IP_ADDRESS`` with the IP address of your NFS server.

#. To make the mount persistent across reboots, add the following line to ``/etc/fstab``:

   .. code-block:: none

      NFS_SERVER_IP_ADDRESS:/mnt/nfs  /mnt/nfs  nfs  defaults,nofail  0  0