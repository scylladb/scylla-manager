=============
Specification
=============

.. contents::
   :depth: 1
   :local:

Directory Layout
----------------

The Scylla Manager backup requires a backup location string that specifies the storage provider and name of a bucket (using Amazon S3 naming) ex. ``s3:<your S3 bucket name>``.
In that bucket Scylla Manager creates a ``buckup`` directory where all the backup data and metadata are stored.

There are three subdirectories:

* meta - contains backup manifests
* schema - contains text dumps of database schema
* sst - contains the data files

meta
....

The meta directory contains backup manifests.
Please find below a listing of meta directory with three nodes and three runs of a backup task.

.. code-block:: none

   meta
   └── cluster
       └── 3e99d4a8-67d2-45fe-87fb-87b1b90ea2dc
           └── dc
               └── dc1
                   └── node
                       ├── 01c9349e-89e6-4ceb-a727-4f27f9f2acce
                       │   ├── task_f70117d8-c10e-4e90-9606-2587936b3757_tag_sm_20210809095541UTC_manifest.json.gz
                       │   ├── task_f70117d8-c10e-4e90-9606-2587936b3757_tag_sm_20210809095542UTC_manifest.json.gz
                       │   └── task_f70117d8-c10e-4e90-9606-2587936b3757_tag_sm_20210809095748UTC_manifest.json.gz
                       ├── 427be6b6-0773-465c-b1a6-4ed2265500fe
                       │   ├── task_f70117d8-c10e-4e90-9606-2587936b3757_tag_sm_20210809095541UTC_manifest.json.gz
                       │   ├── task_f70117d8-c10e-4e90-9606-2587936b3757_tag_sm_20210809095542UTC_manifest.json.gz
                       │   └── task_f70117d8-c10e-4e90-9606-2587936b3757_tag_sm_20210809095748UTC_manifest.json.gz
                       └── ae6a5cf3-cb53-4954-8c16-866003727111
                           ├── task_f70117d8-c10e-4e90-9606-2587936b3757_tag_sm_20210809095541UTC_manifest.json.gz
                           ├── task_f70117d8-c10e-4e90-9606-2587936b3757_tag_sm_20210809095542UTC_manifest.json.gz
                           └── task_f70117d8-c10e-4e90-9606-2587936b3757_tag_sm_20210809095748UTC_manifest.json.gz

Each node has it's own directory, the path is structured as follows.

.. code-block:: none

   meta/cluster/<cluster ID>/dc/<data center name>/node/<node ID>

The directory is flat and contains all the manifests, the file name is structured as follows.

.. code-block:: none

   task_<task ID>_tag_<snapshot tag>_manifest.json.gz

You may find that the directory contains manifest files with ``.tmp`` suffix.
Those are manifests files of backups that are being uploaded to the backup location.
They are are marked as temporary until all the backup files are fully uploaded.

.. _backup-schema-spec:

schema
......

The schema directory contains text dumps of database schema.

Starting from ScyllaDB 6.0 and 2024.2 (and compatible ScyllaDB Manager 3.3), they are required for the schema restoration.
They have ``schema_with_internals.json.gz`` suffix and represent the output of ``DESCRIBE SCHEMA WITH INTERNALS`` CQL query,
which returns rows in the format ``keyspace|type|name|CQL create statement`` in the correct order.

If you are using an earlier ScyllaDB version, those files are mainly for information purposes.
To restore the backup, use the ``system_schema`` keyspace.
They have ``schema.tar.gz`` suffix and represent schema archive divided among keyspaces.

To enable upload of the files make sure that the cluster is added with username and password flags.

.. code-block:: none

   schema
   └── cluster
       └── 3e99d4a8-67d2-45fe-87fb-87b1b90ea2dc
           ├── task_f70117d8-c10e-4e90-9606-2587936b3757_tag_sm_20210809095541UTC_schema_with_internals.json.gz
           ├── task_f70117d8-c10e-4e90-9606-2587936b3757_tag_sm_20210809095542UTC_schema_with_internals.json.gz
           └── task_f70117d8-c10e-4e90-9606-2587936b3757_tag_sm_20210809095748UTC_schema_with_internals.json.gz

The schema file path is structured as follows.

.. code-block:: none

   meta/cluster/<cluster ID>/task_<task ID>_tag_<snapshot tag>_schema_with_internals.json.gz

sst
...

The sst directory contains the data files.
Please find below a part listing of sst directory showing a single table of a single node.

.. code-block:: none

   sst/
   └── cluster
       └── 3e99d4a8-67d2-45fe-87fb-87b1b90ea2dc
           └── dc
               └── dc1
                   └── node
                       ├── 01c9349e-89e6-4ceb-a727-4f27f9f2acce
                       │   └── keyspace
                       │       └── backuptest_data
                       │           └── table
                       │               └── big_table
                       │                   └── f34b6ff0f8f711eb9fcf000000000000
                       │                       ├── mc-2-big-CompressionInfo.db
                       │                       ├── mc-2-big-Data.db
                       │                       ├── mc-2-big-Digest.crc32
                       │                       ├── mc-2-big-Filter.db
                       │                       ├── mc-2-big-Index.db
                       │                       ├── mc-2-big-Scylla.db
                       │                       ├── mc-2-big-Statistics.db
                       │                       ├── mc-2-big-Summary.db
                       │                       ├── mc-2-big-TOC.txt
                       │                       ├── mc-3-big-CompressionInfo.db
                       │                       ├── mc-3-big-Data.db
                       │                       ├── mc-3-big-Digest.crc32
                       │                       ├── mc-3-big-Filter.db
                       │                       ├── mc-3-big-Index.db
                       │                       ├── mc-3-big-Scylla.db
                       │                       ├── mc-3-big-Statistics.db
                       │                       ├── mc-3-big-Summary.db
                       │                       ├── mc-3-big-TOC.txt
                       │                       ├── mc-4-big-CompressionInfo.db
                       │                       ├── mc-4-big-Data.db
                       │                       ├── mc-4-big-Digest.crc32
                       │                       ├── mc-4-big-Filter.db
                       │                       ├── mc-4-big-Index.db
                       │                       ├── mc-4-big-Scylla.db
                       │                       ├── mc-4-big-Statistics.db
                       │                       ├── mc-4-big-Summary.db
                       │                       ├── mc-4-big-TOC.txt
                       │                       ├── mc-5-big-CompressionInfo.db
                       │                       ├── mc-5-big-Data.db
                       │                       ├── mc-5-big-Digest.crc32
                       │                       ├── mc-5-big-Filter.db
                       │                       ├── mc-5-big-Index.db
                       │                       ├── mc-5-big-Scylla.db
                       │                       ├── mc-5-big-Statistics.db
                       │                       ├── mc-5-big-Summary.db
                       │                       ├── mc-5-big-TOC.txt
                       │                       ├── mc-5-big-CompressionInfo.db.sm_20210809095541UTC
                       │                       ├── mc-5-big-Data.db.sm_20210809095541UTC
                       │                       ├── mc-5-big-Digest.crc32.sm_20210809095541UTC
                       │                       ├── mc-5-big-Filter.db.sm_20210809095541UTC
                       │                       ├── mc-5-big-Index.db.sm_20210809095541UTC
                       │                       ├── mc-5-big-Scylla.db.sm_20210809095541UTC
                       │                       ├── mc-5-big-Statistics.db.sm_20210809095541UTC
                       │                       ├── mc-5-big-Summary.db.sm_20210809095541UTC
                       │                       └── mc-5-big-TOC.txt.sm_20210809095541UTC

Each node has it's own directory, the path is structured as follows.

.. code-block:: none

   sst/cluster/<cluster ID>/dc/<data center name>/node/<node ID>

Under the node directory each table version has it's directory, the path is structured as follows.

.. code-block:: none

   keyspace/<keyspace name>/table/<table name>/<table schema version>

The directory contains all the table files.
Some files may be used in more than one backup.

You may also find that some of those files have the snapshot tag suffix (e.g. ``.sm_20210809095541UTC``).
Even though sstables (data files) are immutable by nature,
`replacing a running node <https://opensource.docs.scylladb.com/stable/operating-scylla/procedures/cluster-management/replace-running-node.html#replace-a-running-node-by-taking-its-place-in-the-cluster>`_
makes it possible to have two different sstables originating
from the same table and with the same node ID. In this case, older versions of sstable are appended with the snapshot tag
indicating the backup that introduced newer version of given sstable. The most current version of sstable does not have the snapshot tag suffix.

In the example above, we can see that sstable ``mc-5-big-Data.db`` will be used when restoring snapshot ``sm_20210809095541UTC``,
while ``mc-5-big-Data.db.sm_20210809095541UTC`` will be used when restoring ``sm_20210809095542UTC`` or ``sm_20210809095748UTC`` snapshots
(lexicographical order on snapshot tags is also the chronological order).

Manifest File
-------------

Scylla Manager Manifest files are gzipped JSON files.
Each node has it's own manifest file.
If a cluster has three nodes a backup would contain three manifest files with the same name but under different directories.
Please find below the contents of the manifest file of the node shown in the sst section.

.. code-block:: none

   {
     "version": "v2",
     "cluster_name": "test_cluster",
     "ip": "192.168.100.13",
     "index": [
       {
         "keyspace": "backuptest_data",
         "table": "big_table",
         "version": "f34b6ff0f8f711eb9fcf000000000000",
         "files": [
           "mc-2-big-CompressionInfo.db",
           "mc-2-big-Data.db",
           "mc-2-big-Digest.crc32",
           "mc-2-big-Filter.db",
           "mc-2-big-Index.db",
           "mc-2-big-Scylla.db",
           "mc-2-big-Statistics.db",
           "mc-2-big-Summary.db",
           "mc-2-big-TOC.txt",
           "mc-3-big-CompressionInfo.db",
           "mc-3-big-Data.db",
           "mc-3-big-Digest.crc32",
           "mc-3-big-Filter.db",
           "mc-3-big-Index.db",
           "mc-3-big-Scylla.db",
           "mc-3-big-Statistics.db",
           "mc-3-big-Summary.db",
           "mc-3-big-TOC.txt"
         ],
         "size": 1256031
       }
     ],
     "size": 1256031,
     "tokens": [
       -9214072223864974000,
       -9209525598183111000,
       -9203154907091372000,
       -9121005554342506000,
       -9077102529676286000,
       -8972056514211153000,
       -8928968163169332000,
       -8928882009746142000,
       -8863551618551160000,
       -8857300148094569000,
       ...],
     "schema": "backup/schema/cluster/3e99d4a8-67d2-45fe-87fb-87b1b90ea2dc/task_f70117d8-c10e-4e90-9606-2587936b3757_tag_sm_20210809095541UTC_schema.tar.gz"
   }

The manifest contains the following information.

* version - the version of the manifest
* cluster_name - name of the cluster as registered in Scylla Manager
* ip - public IP address of the node
* index - list of tables, each table holds a list of file names
* size - total size of files in index
* tokens - tokens owned by node, they allow to recreate the cluster topology
* schema - path to schema file
