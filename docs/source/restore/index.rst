=======
Restore
=======

.. toctree::
   :hidden:
   :maxdepth: 2

   extract-schema-from-metadata
   extract-schema-from-system-schema-keyspace

.. contents::
   :depth: 2
   :local:

Restoring data from Scylla Manager backup is not yet automated.
This document provides information on how to manually restore data from Scylla Manager backups.
There are two restore scenarios covered here:

#. Backup to the same cluster.
#. Backup to a different cluster using sstableloader

**Prerequisites**

#. Destination Scylla cluster is up and running.
#. Scylla Manager Agent is installed on all nodes of the cluster.
#. Backup location is accessible from all the nodes.
#. The cluster is added to Scylla Manager.

You need a working Scylla Manager setup to list backups.
If you don't have it installed please follow official instructions on :ref:`Scylla Manager Installation <install-manager>` first.

Identify relevant snapshot
==========================

List all available backups and choose the one you would like to restore.
Run: `sctool backup list <../sctool/#backup-list>`_, to lists all backups for the cluster.
This command will list backups only created with provided cluster (``-c new-cluster``).
If you don't have UUID of the old cluster you can use ``--all-clusters`` flag to list backups from all clusters that are available in the backup location.

Example:

.. code-block:: none

   sctool backup list -c new-cluster --all-clusters -L s3:backup-bucket
   Cluster: 7313fda0-6ebd-4513-8af0-67ac8e30077b

   Snapshots:
   - sm_20200513131519UTC (563.07GiB)
   - sm_20200513080459UTC (563.07GiB)
   - sm_20200513072744UTC (563.07GiB)
   - sm_20200513071719UTC (563.07GiB)
   - sm_20200513070907UTC (563.07GiB)
   - sm_20200513065522UTC (563.07GiB)
   - sm_20200513063046UTC (563.16GiB)
   - sm_20200513060818UTC (534.00GiB)
   Keyspaces:
   - system_auth (4 tables)
   - system_distributed (2 tables)
   - system_schema (12 tables)
   - system_traces (5 tables)
   - user_data (100 tables)

In this example we have eight different snapshots to choose from.
Snapshot tags encode date they were taken in UTC time zone.
For example, ``sm_20200513131519UTC`` was taken on 13/05/2020 at 13:15 and 19 seconds UTC.
The data source for the listing is the cluster backup locations.
Listing may take some time depending on how big the cluster is and how many backups there are.

Restore schema
==============

The easiest approach to restoring schema is to restore the ``system_schema`` keyspace together with the user data.
In that case there is nothing to do and you can move on to the next section.
In some rare cases, like restoring only single table, you may want to extract elements of the schema, recreate it manually, and only then restore data.
To do that there are two options:

* If cluster was :ref:`added with CQL credentials <add-cluster>` Scylla Manager backups the schema in CQL text format and you may :doc:`Extract schema from metadata <extract-schema-from-metadata>`.
* Otherwise you must :doc:`Extract schema from system_schema keyspace <extract-schema-from-system-schema-keyspace>`.

When you have the schema as a CQL file, recreate the schema from one of the nodes.

.. code-block:: none

   cqlsh -e "SOURCE '/path_to_schema/<schema_name.cql>'"

Prepare to restore
==================

Repeat the following steps for each node in the cluster.

**Procedure**

#. Run the ``nodetool drain`` command to ensure the data is flushed to the SSTables.

#. Shut down the node:

   .. code-block:: none

      sudo systemctl stop scylla-server

#. Delete all the files in the commitlog. Deleting the commitlog will prevent the newer insert from overriding the restored data.

   .. code-block:: none

      sudo rm -rf /var/lib/scylla/commitlog/*

#. Delete all the files in the keyspace_name_table. Note that by default the snapshots are created under Scylla data directory.

   .. code-block:: none

      /var/lib/scylla/data/keyspace_name/table_name-UUID/

   Make sure NOT to delete the existing snapshots in the process.

   For example:

   .. code-block:: none

      sudo ll /var/lib/scylla/data/mykeyspace/team_players-6e856600017f11e790f4000000000000

      -rw-r--r-- 1 scylla   scylla     66 Mar  5 09:19 nba-team_players-ka-1-CompressionInfo.db
      -rw-r--r-- 1 scylla   scylla    669 Mar  5 09:19 nba-team_players-ka-1-Data.db
      -rw-r--r-- 4 scylla   scylla     10 Mar  5 08:46 nba-team_players-ka-1-Digest.sha1
      -rw-r--r-- 1 scylla   scylla     24 Mar  5 09:19 nba-team_players-ka-1-Filter.db
      -rw-r--r-- 1 scylla   scylla    218 Mar  5 09:19 nba-team_players-ka-1-Index.db
      -rw-r--r-- 1 scylla   scylla     38 Mar  5 09:19 nba-team_players-ka-1-Scylla.db
      -rw-r--r-- 1 scylla   scylla   4446 Mar  5 09:19 nba-team_players-ka-1-Statistics.db
      -rw-r--r-- 1 scylla   scylla     89 Mar  5 09:19 nba-team_players-ka-1-Summary.db
      -rw-r--r-- 4 scylla   scylla    101 Mar  5 08:46 nba-team_players-ka-1-TOC.txt
      drwx------ 5 scylla   scylla     69 Mar  6 08:14 snapshots
      drwx------ 2 scylla   scylla      6 Mar  5 08:40 upload

      sudo rm -f  /var/lib/scylla/data/mykeyspace/team_players-6e856600017f11e790f4000000000000/*

      rm: cannot remove ‘/var/lib/scylla/data/nba/team_roster-c019f8108fda11e8b16a000000000001/snapshots’: Is a directory
      rm: cannot remove ‘/var/lib/scylla/data/nba/team_roster-c019f8108fda11e8b16a000000000001/upload’: Is a directory

      sudo ll /var/lib/scylla/data/mykeyspace/team_players-6e856600017f11e790f4000000000000/

      drwx------ 5 scylla   scylla     69 Mar  6 08:14 snapshots
      drwx------ 2 scylla   scylla      6 Mar  5 08:40 upload

Upload data to Scylla
=====================

To the same cluster
-------------------

List the backup files
.....................

List the backup files needed on each node and save the list to a file.

If you are listing old backups from the new cluster use ``--all-clusters`` parameter.

.. code-block:: none

   sctool backup files -c cluster1 --snapshot-tag sm_20200513131519UTC \
   --with-version \
   --location s3:backup-bucket \
    > backup_files.out

Snapshot information is now stored in ``backup_files.out`` file.
Each line of the ``backup_files.out`` file contains mapping between path to the SSTable file in the backup bucket and it's mapping to keyspace/table.
If Scylla Manager is configured to store database schemas with the backups then first line in the file listing is path to the schema archive.

For example:

.. code-block:: none

   s3://backup-bucket/backup/sst/cluster/7313fda0-6ebd-4513-8af0-67ac8e30077b/dc/AWS_EU_CENTRAL_1/node/92de78b1-6c77-4788-b513-2fff5a178fe5/keyspace/user_data/table/data_65/a2667040944811eaaf9d000000000000/la-72-big-Index.db 	 user_data/data_65-a2667040944811eaaf9d000000000000

Path contains metadata, for example:

* Cluster ID - 7313fda0-6ebd-4513-8af0-67ac8e30077b
* Data Center - AWS_EU_CENTRAL_1
* Directory - /var/lib/scylla/data/user_data/data_65-a2667040944811eaaf9d000000000000/
* Keyspace - user_data

.. code-block:: none

   sctool backup files -c prod-cluster --snapshot-tag sm_20191210145027UTC \
   --with-version > backup_files.out

Each line describes a backed-up file and where it should be downloaded. For example

.. code-block:: none

   s3://backups/backup/sst/cluster/1d781354-9f9f-47cc-ad45-f8f890569656/dc/dc1/node/ece658c2-e587-49a5-9fea-7b0992e19607/keyspace/auth_service/table/roles/5bc52802de2535edaeab188eecebb090/mc-2-big-CompressionInfo.db      auth_service/roles-5bc52802de2535edaeab188eecebb090

This file has to be copied to:

* Cluster - 1d781354-9f9f-47cc-ad45-f8f890569656
* Data Center - dc1
* Node - ece658c2-e587-49a5-9fea-7b0992e19607
* Directory - /var/lib/scylla/data/auth_service/roles-5bc52802de2535edaeab188eecebb090/upload

Download the backup files
.........................

This step must be executed on **each node** in the cluster.

#. Copy ``backup_files.out`` file as ``/tmp/backup_files.out`` on the node.

#. Run ``nodetool status`` to get to know the node ID.

#. Download data into table directories.
   As the file is kept in S3 so we can use S3 CLI to download it (this step may be different with other storage providers).
   Grep can be used to filter specific files to restore.
   With node UUID we can filter files only for a single node.
   With keyspace name we can filter files only for a single keyspace.

   .. code-block:: none

      cd /var/lib/scylla/data

      # Filter only files for a single node.
      grep ece658c2-e587-49a5-9fea-7b0992e19607 /tmp/backup_files.out | xargs -n2 aws s3 cp

#. Make sure that all files are owned by the scylla user and group.
   We must ensure that permissions are right after copy:

   .. code-block:: none

      sudo chown -R scylla:scylla /var/lib/scylla/data/user_data/

#. Start the Scylla nodes:

   .. code-block:: none

      sudo systemctl start scylla-server

Repair
......

After performing the above on all nodes, repair the cluster with Scylla Manager Repair.
This makes sure that the data is consistent on all nodes and between each node.


To a new cluster
----------------

In order to restore backup to cluster which has a different topology, you have to use an external tool called `sstableloader <https://docs.scylladb.com/operating-scylla/procedures/cassandra_to_scylla_migration_process/>`_.
This procedure is much slower than restoring to the same topology cluster.

**Procedure**

#. Start up the nodes if they are not running after schema restore:

   .. code-block:: none

      sudo systemctl start scylla-server

#. List all the backup files and save the list to a file.

   Use ``--all-clusters`` if you are restoring from the cluster that no longer exists.

   .. code-block:: none

      sctool backup files -c cluster1 --snapshot-tag sm_20200513131519UTC --location s3:backup-bucket > backup_files.out

#. Copy ``backup_files.out`` file as ``/tmp/backup_files.out`` on the host where ``sstableloader`` is installed.

#. Download all files created during backup into temporary location:

   .. code-block:: none

      mkdir snapshot
      cd snapshot
      # Create temporary directory structure.
      cat /tmp/backup_files.out | awk '{print $2}' | xargs mkdir -p
      # Download snapshot files.
      cat /tmp/backup_files.out | xargs -n2 aws s3 cp

#. Execute following command for each table by providing list of node IP addresses and path to sstable files on node that has sstableloader installed:

   .. code-block:: none

      # Loads table user_data.data_0 into four node cluster.
      sstableloader -d '35.158.14.221,18.157.98.72,3.122.196.197,3.126.2.205' ./user_data/data_0 --username scylla --password <password>

After tables are restored verify validity of your data by running queries on your database.
