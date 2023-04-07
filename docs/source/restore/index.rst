=======
Restore
=======

.. toctree::
   :hidden:
   :maxdepth: 2

   restore-tables
   restore-schema
   examples
   compatibility-matrix

.. contents::
   :depth: 2
   :local:

The :ref:`sctool restore <sctool-restore>` command allows you to run a restore of backed-up data (identified by its snapshot-tag) into a cluster.
Restore and backups are scheduled in the same manner: you can start, stop, resume, and track task progress on demand.

.. note:: Restore procedure works with any cluster topologies, so backed-up cluster can have different number of nodes than restore destination cluster.

Restore types
=============

Restore task has to be one of two types:

  * :doc:`restore tables <restore-tables>` - restores the content of the tables (rows)

  * :doc:`restore schema <restore-schema>` - restores the ScyllaDB cluster schema

Each of those types has required prerequisites and follow-up actions.
For more information, please read given restore type documentation.

If both the schema and the content of the tables need to be restored, you must start with restoring the schema. Only after the schema is successfully restored can you proceed with restoring the content of the tables.

Features
========

ScyllaDB Manager Restore command supports the following features:

* Glob patterns to select keyspaces or tables to restore
* Control over the :ref:`restore speed and granularity <restore-speed-and-granularity>`
* :ref:`Dry run <restore-dry-run>` - Test restore before live execution
* Progress tracking (:ref:`sctool progress <task-progress>`, Prometheus metrics, `Scylla Monitoring <https://monitoring.docs.scylladb.com>`_ Manager dashboard)
* :ref:`Pausing <task-stop>` and :ref:`resuming <task-start>` at any point of the process

Restore speed and granularity
=============================

.. _restore-speed-and-granularity:

Restore speed is controlled by two parameters: ``--parallel`` and ``--batch-size``.
Parallel specifies how many nodes can be used in restore procedure at the same time.
Batch size specifies how many SSTable bundles can be restored from backup location in a single job.
Those parameters can be set when you:

* Schedule a restore with :ref:`sctool restore <sctool-restore>`
* Update a restore specification with :ref:`sctool restore update <restore-update>`
