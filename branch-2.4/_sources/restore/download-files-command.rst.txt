======================
Download files command
======================

Scylla Manger Agent comes with a download-files subcommand that given a backup location can be used to:

#. List clusters, datacenters and nodes.
#. Search for snapshot tags.
#. Download files without a need for third-party tools.

Download-files
==============

**Syntax:**

.. code-block:: none

    scylla-manager-agent download-files --location <backup location> [OPTION]...

.. _download-files-parameters:

parameters
..........

.. _download-files-param-clear-tables:

``--clear-tables``
^^^^^^^^^^^^^^^^^^

Remove sstables before downloading

====

.. _download-files-param-config-file:

``-c, --config-file path``
^^^^^^^^^^^^^^^^^^^^^^^^^^

Configuration file path, you can specify the flag multiple times to overwrite configuration options.

**Default:** /etc/scylla-manager-agent/scylla-manager-agent.yaml

====

.. _download-files-param-data-dir:

``-d, --data-dir path``
^^^^^^^^^^^^^^^^^^^^^^^

Path to Scylla data directory (typically /var/lib/scylla/data) or other directory to use for downloading the files.

**Default:** current directory

====

.. _download-files-param-debug:

``--debug``
^^^^^^^^^^^

Enable debug logs.

**Default:** current directory

====

.. _download-files-param-dry-run:

``--dry-run``
^^^^^^^^^^^^^

Validate and print a plan without downloading (or clearing) any files.

====

.. _download-files-param-dump-manifest:

``--dump-manifest``
^^^^^^^^^^^^^^^^^^^

Print Scylla Manager backup manifest as JSON.

====

.. _download-files-param-dump-tokens:

``--dump-tokens``
^^^^^^^^^^^^^^^^^

Print list of tokens from the manifest.

====

.. _download-files-param-keyspace:

``-K, --keyspace list``
^^^^^^^^^^^^^^^^^

A comma-separated list of keyspace/tables glob patterns, e.g. 'keyspace,!keyspace.table_prefix_*'.

====

.. _download-files-param-list-nodes:

``--list-nodes``
^^^^^^^^^^^^^^^^^

Print list of nodes including cluster name and node IP, this command would help you find nodes you can restore data from.

====

.. _download-files-param-list-snapshots:

``--list-snapshots``
^^^^^^^^^^^^^^^^^

Print list of snapshots of the specified node, this also takes into account keyspace filter and returns only snapshots containing any of requested keyspaces or tables, newest snapshots are printed first.

====

.. _download-files-param-location:

``-L, --location string``
^^^^^^^^^^^^^^^^^

Backup location in the format <provider>:<name> e.g. s3:my-bucket, the supported providers are: s3, gcs, azure.

====

.. _download-files-param-mode:

``--mode upload, sstableloader``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Mode changes resulting directory structure, supported values are: upload, sstableloader, set 'upload' to use table upload directories, set 'sstableloader' for <keyspace>/<table> directories layout.

====

.. _download-files-param-node:

``-n, --node ID``
^^^^^^^^^^^^^^^^^

'Host ID' value from nodetool status command output of a node you want to restore.

**Default:** local node

====

.. _download-files-param-parallel:

``-p, --parallel int``
^^^^^^^^^^^^^^^^^^^^^^

How many files to download in parallel.

**Default:** 8

====

.. _download-files-param-rate-limit:

``--rate-limit int``
^^^^^^^^^^^^^^^^^^^^^^

Rate limit in megabytes (MiB) per second.

**Default:** no limit

====

.. _download-files-param-rate-limit:

``-T, --snapshot-tag tag``
^^^^^^^^^^^^^^^^^^^^^^^^^^

Scylla Manager snapshot tag as read from backup listing e.g. sm_20060102150405UTC, use --list-snapshots to get a list of snapshots of the node.
