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

      scylla-manager-agent check-location --location localstorage:<your bucket name>

#. Restart ScyllaDB Manager Agent service.

   .. code-block:: none

      sudo systemctl restart scylla-manager-agent

Troubleshoot connectivity
=========================

To troubleshoot ScyllaDB node to local storage connectivity issues you can run:

.. code-block:: none

   scylla-manager-agent check-location --debug --location localstorage:<your bucket name>
