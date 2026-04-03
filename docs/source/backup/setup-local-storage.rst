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
This is required because ScyllaDB Manager expects that backup data written by any node in a datacenter is accessible by all other nodes in that datacenter.

A common way to achieve this is by using NFS (Network File System).

Setup shared filesystem with NFS
================================

Below is a simplified NFS setup.
For production deployments, consult the official NFS documentation:

* `Ubuntu NFS guide <https://ubuntu.com/server/docs/network-file-services-nfs>`_
* `RHEL/CentOS NFS guide <https://docs.redhat.com/en/documentation/red_hat_enterprise_linux/9/html/managing_file_systems/exporting-nfs-shares_managing-file-systems>`_

NFS server
----------

On the NFS server, install the NFS server package and export the backup directory.

#. Install the NFS server package.

   .. tabs::

      .. group-tab:: Ubuntu/Debian

         .. code-block:: none

            sudo apt-get install nfs-kernel-server

      .. group-tab:: RHEL/CentOS

         .. code-block:: none

            sudo yum install nfs-utils

#. Create the backup directory.

   .. code-block:: none

      sudo mkdir -p /mnt/nfs/scylla-backups
      sudo chown nobody:nogroup /mnt/nfs/scylla-backups

#. Export the directory by adding the following line to ``/etc/exports``.
   Replace ``<subnet>`` with the network range of your ScyllaDB nodes (e.g. ``192.168.1.0/24``).

   .. code-block:: none

      /mnt/nfs/scylla-backups <subnet>(rw,sync,no_subtree_check)

#. Apply the export configuration and start the NFS server.

   .. code-block:: none

      sudo exportfs -a
      sudo systemctl restart nfs-kernel-server

NFS client (each ScyllaDB node)
-------------------------------

On **each** ScyllaDB node that will participate in backups, mount the NFS share.

#. Install the NFS client package.

   .. tabs::

      .. group-tab:: Ubuntu/Debian

         .. code-block:: none

            sudo apt-get install nfs-common

      .. group-tab:: RHEL/CentOS

         .. code-block:: none

            sudo yum install nfs-utils

#. Create the mount point.

   .. code-block:: none

      sudo mkdir -p /mnt/nfs/scylla-backups

#. Mount the NFS share. Replace ``<nfs-server-ip>`` with the IP address of your NFS server.

   .. code-block:: none

      sudo mount <nfs-server-ip>:/mnt/nfs/scylla-backups /mnt/nfs/scylla-backups

#. To make the mount persist across reboots, add the following line to ``/etc/fstab``:

   .. code-block:: none

      <nfs-server-ip>:/mnt/nfs/scylla-backups /mnt/nfs/scylla-backups nfs defaults 0 0

Configure the agent
===================

This procedure needs to be repeated for each ScyllaDB node.

**Procedure**

Edit the ``/etc/scylla-manager-agent/scylla-manager-agent.yaml``

#. Uncomment the ``localstorage:`` line, for parameters note the two spaces in front, it's a yaml file.
#. Uncomment and set ``path`` to the absolute path of the shared filesystem mount point (e.g. ``/mnt/nfs/scylla-backups``).

   .. code-block:: yaml

      localstorage:
        path: /mnt/nfs/scylla-backups

#. Validate that the agent has access to the backup location.
   If there is no response, the location is accessible. If not, you will see an error.

   .. code-block:: none

      scylla-manager-agent check-location --location localstorage:<your backup name>

#. Restart ScyllaDB Manager Agent service.

   .. code-block:: none

      sudo systemctl restart scylla-manager-agent

Troubleshoot connectivity
=========================

To troubleshoot ScyllaDB node to local storage connectivity issues you can run:

.. code-block:: none

   scylla-manager-agent check-location --debug --location localstorage:<your backup name>
