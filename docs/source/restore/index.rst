=======
Restore
=======

.. toctree::
   :hidden:
   :maxdepth: 2

   restore-tables
   restore-schema
   old-restore-schema
   examples
   compatibility-matrix

.. contents::
   :depth: 2
   :local:

The :ref:`sctool restore <sctool-restore>` command allows you to run a restore of backed-up data (identified by its snapshot-tag) into a cluster.
Restore and backups are scheduled in the same manner: you can start, stop, resume, and track task progress on demand.

.. note:: If you are using ScyllaDB Manager deployed by ScyllaDB Operator, see dedicated `ScyllaDB Operator documentation <https://operator.docs.scylladb.com/stable/nodeoperations/restore.html#>`_.

.. warning::
    Restore works only when ScyllaDB version of backed-up cluster is the same or older than ScyllaDB version of restore destination cluster.

    Mixing ScyllaDB Enterprise and ScyllaDB Open Source versions could lead to unexpected results.

Restore types
=============

Restore task has to be one of two types:

  * :doc:`restore tables <restore-tables>` - restores the content of the tables (rows)

  * :doc:`restore schema <restore-schema>` - restores the ScyllaDB cluster schema

If both the schema and the content of the tables need to be restored, you must start with restoring the schema. Only after the schema is successfully restored can you proceed with restoring the content of the tables.

Features
========

ScyllaDB Manager Restore command supports the following features:

* Glob patterns to select keyspaces or tables to restore
* Control over the :ref:`restore speed and granularity <restore-speed-and-granularity>`
* :ref:`Dry run <restore-dry-run>` - Test restore before live execution
* Progress tracking (:ref:`sctool progress <task-progress>`, Prometheus metrics, `Scylla Monitoring <https://monitoring.docs.scylladb.com>`_ Manager dashboard)
* :ref:`Pausing <task-stop>` and :ref:`resuming <task-start>` at any point of the process

Restore speed observability
===========================

| Restore speed can be checked with :ref:`sctool progress <task-progress>` command.
| It displays average per shard download and load&stream bandwidths. When used with ``--details`` flag, it also displays per host bandwidths.

Restore speed can be also observed with Prometheus metrics:

* ``scylla_manager_restore_remaining_bytes``
* ``scylla_manager_restore_downloaded_bytes``
* ``scylla_manager_restore_download_duration``
* ``scylla_manager_restore_streamed_bytes``
* ``scylla_manager_restore_stream_duration``

Restore speed control
=====================

.. _restore-speed-and-granularity:

Restore speed is controlled by many parameters (see :ref:`sctool restore <sctool-restore>` documentation for details):

* ``--batch-size``
* ``--parallel``
* ``--transfers``
* ``--rate-limit``
* ``--unpin-agent-cpu``
* ``--allow-compaction``

| Most of those parameters have default values chosen for restoring as fast as possible.
| You should need to change them only when you want to limit the impact that the restore has on a cluster serving traffic on not currently restored tables.

| For backward compatibility reasons, the default value of ``--batch-size`` is ``2``, but it should be changed to ``0`` when you want to maximize restore speed.
| Note that with bigger batch size comes lesser granularity. This means that pausing and resuming restore would need to perform more work.

The ``--unpin-agent-cpu`` is disabled by default, but in case you observe small download
bandwidth, you could try to :ref:`pause <task-stop>` restore task, :ref:`update <restore-update>` it with ``--unpin-agent-cpu``,
and :ref:`resume <task-start>` it.
