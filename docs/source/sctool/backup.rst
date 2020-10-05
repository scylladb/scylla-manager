Backup
------

The backup commands allow you to: create and update a backup (ad-hoc or scheduled), list the contents of a backup, and list the backups of a cluster.
You cannot initiate a backup without a cluster. Make sure you add a cluster (:ref:`cluster-add`) before initiating a backup.

.. code-block:: none

   sctool backup <subcommand> [global flags] [parameters]


**Subcommands**

.. list-table::
   :widths: 30 70
   :header-rows: 1

   * - Command
     - Usage
   * - `backup`_
     - Schedule a backup (ad-hoc or scheduled).
   * - `backup update`_
     - Modify properties of the existing backup task.
   * - `backup files`_
     - List contents of a given backup.
   * - `backup list`_
     - List backups of a given cluster.
   * - `backup delete`_
     - Deletes one of the available snapshots.

backup
======

The backup command allows you to schedule or run ad-hoc cluster backup.

**Syntax:**

.. code-block:: none

    sctool backup --cluster <id|name> --location <list of locations> [--dc <list>]
    [--dry-run] [--interval <time-unit>]
    [--keyspace <list of glob patterns to find keyspaces>]
    [--num-retries <times to rerun a failed task>]
    [--rate-limit <list of rate limits>] [--retention <number of backups to store>]
    [--show-tables]
    [--snapshot-parallel <list of parallelism limits>] [--start-date <date>]
    [--upload-parallel <list of parallelism limits>] [global flags]

backup parameters
.................

In addition to the :ref:`global-flags`, backup takes the following parameters:


=====

``--dc <list of glob patterns>``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

A comma-separated list of datacenter glob patterns, e.g. 'dc1,!otherdc*' used to specify the DCs to include or exclude from backup, separated by a comma.
This can also include glob patterns.

.. include:: ../_common/glob.rst

=====

``--dry-run``
^^^^^^^^^^^^^

Validates and prints backup information without actually scheduling a backup.

=====

``-i, --interval <time-unit>``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Scheduled Intervals for backups to repeat every X time, where X can be:

* ``d`` - days
* ``h`` - hours
* ``m`` - minutes
* ``s`` - seconds

For example: ``-i 3d2h10m``

**Default: 0** - this means the task does not recur.

=====

``-K, --keyspace <list of glob patterns to find keyspaces>``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

A list of glob patterns separated by a comma used to include or exclude keyspaces from the backup.
The patterns match keyspaces and tables, when you write the pattern,
separate the keyspace name from the table name with a dot (*KEYSPACE.TABLE*).

.. include:: ../_common/glob.rst

=====

``-L, --location <list of backup locations>``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Specifies where to place the backup in the format ``[dc:]<provider>:<name>`` For example: ``s3:my-bucket``.
More than one location can be stated in a comma-separated list.
The <dc>: part is optional and is only needed when different datacenters are being used to upload data to different locations.
``name`` must be an alphanumeric string and **may contain a dash and or a dot, but other characters are forbidden**.
The only supported storage ``provider`` at the moment are ``s3`` and ``gcs``.

=====

``--rate-limit <list of rate limits>``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Limits the upload rate (as expressed in  megabytes (MB) per second) which a snapshot file can be uploaded from a Scylla node to its backup destination.
For example, an S3 bucket.
You can set limits for more than one DC using a comma-separated list expressed in the format ``[<dc>:]<limit>``.
The <dc>: part is optional and is only needed when different datacenters require different upload limits.

**Default: 100**

=====

``--retention <number of backups to store>``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The number of backups to store.
Once this number is reached, the next backup which comes in from this destination will initiate a purge of the oldest backup.

**Default: 3**

=====

``--show-tables``
^^^^^^^^^^^^^^^^^

Prints table names together with keyspace. Used in combination with ``--dry-run``.

=====

``--snapshot-parallel <list of parallelism limits>``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

A comma-separated list of snapshot parallelism limits in the format ``[<dc>:]<limit>``.
More than one location can be stated in a comma-separated list.
The ``dc`` part is optional and allows for specifying different limits in selected datacenters.
If the ``dc`` part is not set, the limit is global and the runs are parallel in ``n`` nodes. If for example, you were to set 'dc1:2,5', then ``dc1`` would have two parallel nodes and there would be five parallel nodes in the other DCs.

=====

``-s, --start-date <date>``
^^^^^^^^^^^^^^^^^^^^^^^^^^^

Specifies the task start date expressed in the RFC3339 format or ``now[+duration]``, e.g. ``now+3d2h10m``, valid units are:

* ``d`` - days
* ``h`` - hours
* ``m`` - minutes
* ``s`` - seconds
* ``now`` - happens immediately

**Default: now**

=====

``--upload-parallel <list of parallelism limits>``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

A comma-separated list of upload parallelism limits in the format ``[<dc>:]<limit>``.
More than one location can be stated in a comma-separated list.
The ``dc`` part is optional and allows for specifying different limits in selected datacenters.
If the ``dc`` part is not set, the limit is global (e.g. 'dc1:2,5') the runs are parallel in ``n`` nodes. In the example in ``dc1`` there are 2 parallel nodes in dc1 and 5 parallel nodes in the other DCs.

=====

Example: backup
................

This example backs up the entire cluster named prod-cluster.
The backup begins on December 9, 2019 at 15:16:05 UTC and will repeat at this time every 24 hours.
The backup is stored in s3 in a directory named ``my-backups``.
Additional examples are available in `Backup Scylla Clusters <../backup/>`_

.. code-block:: none

   sctool backup -c prod-cluster -s '2019-12-09T15:16:05Z' -i 24h -L 's3:my-backups'
   backup/3208ff15-6e8f-48b2-875c-d3c73f545410

backup update
=============

The backup update command allows you to modify properties of an already existing backup task.

**Syntax:**

.. code-block:: none

    sctool backup update <type/task-id> --cluster <id|name> --location <list of locations> [--dc <list>]
    [--dry-run] [--interval <time-unit>]
    [--keyspace <list of glob patterns to find keyspaces>]
    [--rate-limit <list of rate limits>] [--retention <number of backups to store>]
    [--show-tables]
    [--snapshot-parallel <list of parallelism limits>] [--start-date <date>]
    [--upload-parallel <list of parallelism limits>] [global flags]

backup update parameters
........................

In addition to :ref:`global-flags`, backup update takes the same parameters as `backup parameters`_

Example: backup update
......................

This example updates backup task that was previously created.
The backup is updated to run every 12 hours.
The backup storage is updated to ``prod-backups``.

.. code-block:: none

   sctool backup backup/3208ff15-6e8f-48b2-875c-d3c73f545410 -c prod-cluster -i 12h -L 's3:prod-backups'
   backup/3208ff15-6e8f-48b2-875c-d3c73f545410

backup list
===========

This commands allow you to list backups of a given cluster.


**Syntax:**

.. code-block:: none

   sctool backup list [--all clusters] [--keyspace <list of glob patterns to find keyspaces>] [--location <list of backup locations>]
   [--max-date <date>] [--min-date <date>] [--show-tables][global flags]

backup list parameters
......................

In addition to the :ref:`global-flags`, backup list takes the following parameters:

=====

``--all-clusters``
^^^^^^^^^^^^^^^^^^

Shows backups for all clusters. Useful for listing clusters that are no longer available locally but are backed up in the past to remote location.

=====

``-K, --keyspace <list of glob patterns to find keyspaces>``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

A list of glob patterns separated by a comma.
The patterns match keyspaces and tables, when you write the pattern,
separate the keyspace name from the table name with a dot (*KEYSPACE.TABLE*).

.. include:: ../_common/glob.rst

=====

``-L, --location <list of backup locations>``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Specifies where to place the backup in the format ``[<dc>:]<provider>:<name>``.
More than one location can be stated in a comma-separated list.
The <dc>: part is optional and is only needed when different datacenters are being used to upload data to different locations.
``name`` must be an alphanumeric string and **may contain a dash and or a dot, but other characters are forbidden**.
The only supported storage ``provider`` are ``s3`` and ``gcs``.

=====

``--max-date <date>``
^^^^^^^^^^^^^^^^^^^^^

Specifies maximal snapshot date expressed in RFC3339 form or ``now[+duration]``.
For example: ``now+3d2h10m`` Valid units are:

* ``d`` - days
* ``h`` - hours
* ``m`` - minutes
* ``s`` - seconds
* ``now`` - happens immediately

=====

``--min-date <date>``
^^^^^^^^^^^^^^^^^^^^^

Specifies minimal snapshot date expressed in RFC3339 form or ``now[+duration]``.
For example: ``now+3d2h10m`` Valid units are:

* ``d`` - days
* ``h`` - hours
* ``m`` - minutes
* ``s`` - seconds
* ``now`` - happens immediately

=====

``--show-tables``
^^^^^^^^^^^^^^^^^

Prints table names together with keyspace.

=====

Example: backup list
....................

.. code-block:: none

   sctool backup list -c prod-cluster --show-tables
   Snapshots:
     - sm_20191210145143UTC
     - sm_20191210145027UTC
     - sm_20191210144833UTC
   Keyspaces:
     - system_auth (role_members, roles)
     - system_distributed (view_build_status)
     - system_traces (events, node_slow_log, node_slow_log_time_idx, sessions, sessions_time_idx)
     - test_keyspace_dc1_rf2 (void1)
     - test_keyspace_dc1_rf3 (void1)
     - test_keyspace_dc2_rf2 (void1)
     - test_keyspace_dc2_rf3 (void1)
     - test_keyspace_rf2 (void1)
     - test_keyspace_rf3 (void1)

backup files
============

This command allows you to list content of a given backup.
This command lists files that were uploaded during backup procedure.
It outputs the remote paths of files together with keyspace/table information separated by delimiter that you provide.

**Syntax:**

.. code-block:: none

   sctool backup files [--all clusters] [--keyspace <list of glob patterns to find keyspaces>]
   [--location <list of backup locations>] [global flags]

backup files parameters
.......................

In addition to the :ref:`global-flags`, backup files add takes the following parameters:

=====

``--all-clusters``
^^^^^^^^^^^^^^^^^^

Shows backups for all clusters

=====

``-d, --delimiter <delimiter-character>``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Dictates which character will be used as a whitespace between remote file path and information about keyspace and table.

**Default: '\t'**

=====

``-K, --keyspace <list of glob patterns to find keyspaces>``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

A list of glob patterns separated by a comma.
The patterns match keyspaces and tables, when you write the pattern,
separate the keyspace name from the table name with a dot (*KEYSPACE.TABLE*).

.. include:: ../_common/glob.rst

=====

``-L, --location <list of backup locations>``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Specifies where to place the backup in the format ``[<dc>:]<provider>:<name>``.
More than one location can be stated in a comma-separated list.
The <dc>: part is optional and is only needed when different datacenters are being used to upload data to different locations.
``name`` must be an alphanumeric string and **may contain a dash and or a dot, but other characters are forbidden**.
The only supported storage ``provider`` are ``s3`` and ``gcs``.

=====

``-T, --snapshot-tag <tag>``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Snapshot tag as read from the backup listing

=====

Example: backup files
.....................

.. code-block:: none

   sctool backup files --keyspace system_auth

The command output has the  following format:

.. code-block:: none

   <provider>://<bucket-name>/backup/sst/cluster/<cluster-id>/dc/<dc-id>/
   node/<node-id>/keyspace/<keyspace-name>/table/<table-name>/<table-uuid>/
   <filename><delimiter><keyspace-name>/<table-name>

Example:

.. code-block:: none

   s3://backups/backup/sst/cluster/7d8f190f-c98d-4a06-8bb5-ae96633ee69a/dc/dc2/
   node/f3c6386b-6d54-4546-a2e8-627fff62d3af/keyspace/system_sec/table/roles/5bc52802de2535edaeab188eecebb090/
   mc-2-big-TOC.txt system_sec/table

From this information we know the following:

* Provider - s3
* Bucket name - backups
* Cluster ID - 7d8f190f-c98d-4a06-8bb5-ae96633ee69a
* DC - dc2
* Node - f3c6386b-6d54-4546-a2e8-627fff62d3af
* Keyspace - system_sec
* Table name  - roles
* Table UUID - 5bc52802de2535edaeab188eecebb090
* File name -  mc-2-big-TOC.txt
* Delimiter - whitespace character **'  '**
* Keyspace / table name - system_sec/table

See `Restore <../restore-a-backup/>`_ on information how to use these files to restore a backup.

backup delete
=============

This command allows you to delete files that were uploaded during backup procedure.
Deduplicated files are persisted unless their reference count drops to zero.


**Syntax:**

.. code-block:: none

   sctool backup delete --snapshot-tag <snapshot tag> [--location <list of backup locations>] [global flags]

backup delete parameters
........................

In addition to the :ref:`global-flags`, backup delete takes the following parameters:

=====

``-L, --location <list of backup locations>``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Specifies where to look for the backup in the format ``[<dc>:]<provider>:<name>``.
More than one location can be stated in a comma-separated list.
The <dc>: part is optional and is only needed when different datacenters are being used to upload data to different locations.
``name`` must be an alphanumeric string and **may contain a dash and or a dot, but other characters are forbidden**.
The only supported storage ``provider`` are ``s3`` and ``gcs``.

=====

``-T, --snapshot-tag <tag>``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Snapshot tag as read from the backup listing.

=====

Example: backup delete
......................

.. code-block:: none

   sctool backup delete --snapshot-tag sm_20200526115228UTC

The command does not output anything unless an error happens.
