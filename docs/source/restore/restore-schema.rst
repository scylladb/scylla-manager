==============
Restore schema
==============

.. note:: Currently, Scylla Manager supports only entire schema restoration, so ``--keyspace`` flag is not allowed.

.. note:: Schema files are small in size, and they do not benefit from parallel restore, so ``--parallel`` flag is not allowed.

In order to restore Scylla cluster schema use :ref:`sctool restore <sctool-restore>` with ``--restore-schema`` flag.

Prerequisites
=============

* Scylla Manager with CQL credentials to restore destination cluster.

* It is strongly advised to restore schema only into an empty cluster. Otherwise, the restored schema might be overwritten
   by the already existing one and cause unexpected errors.

Follow-up action
================

After successful restore it is important to perform necessary follow-up action. In case of restoring schema,
you should make a `rolling restart <https://docs.scylladb.com/stable/operating-scylla/procedures/config-change/rolling-restart.html>`_ of an entire cluster.
Without the restart, the restored schema might not be visible, and querying it can return various errors.