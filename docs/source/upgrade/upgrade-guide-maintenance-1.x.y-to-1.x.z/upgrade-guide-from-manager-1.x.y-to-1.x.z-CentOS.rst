

========================================================
Upgrade Guide - Scylla Manager 1.x.y to 1.x.z on CentOS
========================================================

Enterprise customers who use Scylla Manager 1.x.y are encouraged to upgrade to 1.x.z.
For new installations please see `Scylla Manager - Download and Install <https://www.scylladb.com/enterprise-download/#manager>`_.
The steps below instruct you how to upgrade the Scylla Manager server while keeping the manager datastore intact.
If you are not running Scylla Manager 1.x.y, do not perform this upgrade procedure. This procedure only covers upgrades from Scylla Manager 1.x.y to 1.x.z.

Please contact `Scylla Enterprise Support <https://www.scylladb.com/product/support/>`_ team with any questions.

Upgrade Notes
=================

* This upgrade brings to sctool a new command: ``status``. This command shows a listing of the individual nodes in the cluster and records the CQL availability on the nodes.

* Health Check - This upgrade introduces a new feature where each node is monitored by Scylla Manager. When Scylla Manager detects that a node is down an alert message is sent to Scylla Monitoring. Alternitively, you can use the ``sctool status``` command to show the live cluster status. 

* Automated health check - When a cluster is added a new health check task is automatically added to the cluster. Following an upgrade, all existing clusters will have an health check task as well. 

* The sctool argument ``interval-days`` has been renamed to ``interval`` as it now supports more granular time units. For example: ``3d2h10m``. The available time units are ``d``, ``h``, ``m``, and ``s``.

* The sctool command ``cluster list`` no longer displays the **host** column in the results table. This was removed because it was easy to be mislead that this node was the only node being used. Adding a cluster (``cluster add``) still takes a ``--host`` argument, but when all the available nodes are discovered they are persisted and used for subsequent interactions with ScyllaDB.



Upgrade Procedure
=================

* Backup the data
* Download and install new packages
* Validate that the upgrade was successful

Backup the Scylla Manager data
-------------------------------
Scylla Manager server persists its data to a Scylla cluster (data store). Before upgrading, backup the ``scylla_manager`` keyspace from Scylla Manager's backend, following this `backup procedure </operating-scylla/procedures/backup-restore/backup/>`_.

Download and install the new release
------------------------------------

.. _upgrade-manager-1.x.y-to-1.x.z-previous-release:

Before upgrading, check what version you are currently running now using ``rpm -q scylla-manager``. You should use the same version that you had previously installed in case you want to :ref:`rollback <upgrade-manager-1.x.z-to-1.x.y-rollback-procedure-centos>` the upgrade.


To upgrade:


1. Update the `Update the Scylla Manager repo: <https://www.scylladb.com/enterprise-download/#manager>`_ to **1.x.y**

2. Run:

.. code:: sh

   sudo yum update scylla-manager -y

3. Reload your shell execute the below command to reload ``sctool`` code completion.

.. code:: sh

   source /etc/bash_completion.d/sctool.bash


Validate
--------
1. Check that Scylla Manager service is running with ``sudo systemctl status scylla-manager.service``. Confirm the service is active (running). If not, then start it with ``systemctl start scylla-manager.service``.
2. Confirm that the upgrade changed the Client and Server version. Run ``sctool version`` and make sure both are 1.x.z version. For example:

.. code-block:: none

   sctool version
   Client version: 1.3.0-0.20181130.03ae248
   Server version: 1.3.0-0.20181130.03ae248

3. Confirm that following the update, that your managed clusters are still present. Run ``sctool cluster list``

.. code-block:: none
  
   sctool cluster list
   ╭──────────────────────────────────────┬──────────┬───────────────╮
   │ cluster id                           │ name     │ssh user       │
   ├──────────────────────────────────────┼──────────┼───────────────┤
   │ db7faf98-7cc4-4a08-b707-2bc59d65551e │ cluster  │scylla-manager │
   ╰──────────────────────────────────────┴──────────┴───────────────╯

4. Confirm that following the upgrade, there is a healtcheck task for each existing cluster. Run ``sctool task list`` to list the tasks.


.. code-block:: none


   sctool task list -c cluster --all
   ╭──────────────────────────────────────────────────┬───────────────────────────────┬──────┬────────────┬────────╮
   │ task                                             │ next run                      │ ret. │ properties │ status │
   ├──────────────────────────────────────────────────┼───────────────────────────────┼──────┼────────────┼────────┤
   │ healthcheck/afe9a610-e4c7-4d05-860e-5a0ddf14d7aa │ 10 Dec 18 20:21 UTC (+15s)    │ 0    │            │ RUNNING│
   │ repair/4d79ee63-7721-4105-8c6a-5b98c65c3e21      │ 12 Dec 18 00:00 UTC (+7d)     │ 3    │            │ NEW    │
   ╰──────────────────────────────────────────────────┴───────────────────────────────┴──────┴────────────┴────────╯

.. _upgrade-manager-1.x.z-to-1.x.y-rollback-procedure-centos:

Rollback Procedure
==================

The following procedure describes a rollback from Scylla Manager 1.x.z to 1.x.y. Apply this procedure if an upgrade from 1.x.y to 1.x.z failed for any reason.

**Warning:** note that you may lose the manged clusters after downgrade. Should this happen, you will need to add the managed clusters clusters manually.

* Downgrade to :ref:`previous release <upgrade-manager-1.x.y-to-1.x.z-previous-release>`
* Start Scylla Manager
* Valdate Scylla Manager version

Downgrade to previous release
-----------------------------
1. Stop Scylla Manager

.. code:: sh

   sudo systemctl stop scylla-manager

2. Drop the ``scylla_manager`` keyspace from the remote datastore

.. code:: sh

   cqlsh -e "DROP KEYSPACE scylla_manager"

3. Remove Scylla Manager repo

.. code:: sh

   sudo rm -rf /etc/yum.repos.d/scylla-manager.repo
   sudo yum clean all

4. Update the `Scylla Manager repo <https://www.scylladb.com/enterprise-download/#manager>`_ to **1.x.y**

5. Install previous version

.. code:: sh

   sudo yum downgrade scylla-manager scylla-manager-server scylla-manager-client -y

Rollback the Scylla Manager database
------------------------------------

1. Start Scylla Manager to reinitialize the data base schema.

.. code:: sh

   sudo systemctl start scylla-manager

2. Stop Scylla Manager to avoid issues while restoring the backup. If you did not perform any backup before upgrading then you are done now and can continue at "Start Scylla Manager".

.. code:: sh

   sudo systemctl stop scylla-manager

3. Restore the database backup if you performed a backup by following the instructions in `Restore from a Backup </operating-scylla/procedures/backup-restore/restore/>`_.
   You can skip step 1 since the Scylla Manager has done this for you.

Start Scylla Manager
--------------------

.. code:: sh

   sudo systemctl start scylla-manager

Validate Scylla Manager Version
-------------------------------

Validate Scylla Manager version:

.. code:: sh

   sctool version

The version should match with the results you had :ref:`previously <upgrade-manager-1.x.y-to-1.x.z-previous-release>`.
