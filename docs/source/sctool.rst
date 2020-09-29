=====================
Sctool CLI Reference
=====================

This document is a complete reference for the sctool CLI.
The commands are in alphabetical order.

.. contents::
   :depth: 2
   :local:

About sctool
------------

``sctool`` is a Command Line Interface (CLI) for the Scylla Manager server.
The server communicates with managed Scylla clusters and performs cluster-wide operations such as automatic repair and backup.

**Usage**

.. code-block:: none

   sctool command [flags] [global flags]

Global flags
============

* ``--api-cert-file <path>`` - specifies the path to HTTPS client certificate used to access the Scylla Manager server
* ``--api-key-file <path>`` - specifies the path to HTTPS client key used to access the Scylla Manager server
* ``--api-url URL`` - URL of Scylla Manager server (default "http://127.0.0.1:5080/api/v1")
* ``-c, --cluster <cluster_name>`` - Specifies the target cluster name or ID
* ``-h, --help`` - Displays help for commands. Use ``sctool [command] --help`` for help about a specific command.

Environment variables
=====================

sctool uses the following environment variables:

* `SCYLLA_MANAGER_CLUSTER` - if set, specifies the default value for the ``-c, --cluster`` flag, in commands that support it.
* `SCYLLA_MANAGER_API_URL` - if set, specifies the default value for the ``--api-url`` flag; it can be useful when using sctool with a remote Scylla Manager server.

The environment variables may be saved in  your ``~/.bashrc`` file so that the variables are set after login.

Cluster registration
--------------------

The cluster commands allow you to add, delete, list, and update clusters.
A Scylla cluster must be added (`cluster add`_) before management tasks can be initiated.

.. code-block:: none

   sctool cluster <command> [flags] [global flags]

**Subcommands**

.. list-table::
   :widths: 30 70
   :header-rows: 1

   * - Subcommand
     - Usage
   * - `cluster add`_
     - Add a cluster to manager.
   * - `cluster delete`_
     - Delete a cluster from manager.
   * - `cluster list`_
     - Show managed clusters.
   * - `cluster update`_
     - Modify a cluster.

.. _cluster-add:

cluster add
===========

This command adds the specified cluster to the manager.
Once a Scylla cluster is added, a weekly repair task is also added.

Before continuing make sure the cluster that you want to add is prepared for it,
see `Add a cluster to Scylla Manager <../add-a-cluster>`_ for instructions.

**Syntax:**

.. code-block:: none

   sctool cluster add --host <node IP> --auth-token <token>[--name <alias>][--without-repair][global flags]

cluster add parameters
......................

In addition to the `Global flags`_, cluster add takes the following parameters:

=====

.. include:: _common/cluster-params.rst

=====

Example: cluster add
....................

This example is only the command that you use to add the cluster to Scylla Manager, not the entire procedure for adding a cluster.
The procedure is detailed in `Add a cluster to Scylla Manager <../add-a-cluster>`_.

.. code-block:: none

   sctool cluster add --host 34.203.122.52 --auth-token "6Es3dm24U72NzAu9ANWmU3C4ALyVZhwwPZZPWtK10eYGHJ24wMoh9SQxRZEluWMc0qDrsWCCshvfhk9uewOimQS2x5yNTYUEoIkO1VpSmTFu5fsFyoDgEkmNrCJpXtfM" --name prod-cluster
   c1bbabf3-cad1-4a59-ab8f-84e2a73b623f
    __
   /  \     Cluster added! You can set it as default, by exporting env variable.
   @  @     $ export SCYLLA_MANAGER_CLUSTER=c1bbabf3-cad1-4a59-ab8f-84e2a73b623f
   |  |     $ export SCYLLA_MANAGER_CLUSTER=prod-cluster
   || |/
   || ||    Now run:
   |\_/|    $ sctool status -c prod-cluster
   \___/    $ sctool task list -c prod-cluster

Example (IPv6):

.. code-block:: none

   sctool cluster add --host 2a05:d018:223:f00:971d:14af:6418:fe2d --auth-token       "6Es3dm24U72NzAu9ANWmU3C4ALyVZhwwPZZPWtK10eYGHJ24wMoh9SQxRZEluWMc0qDrsWCCshvfhk9uewOimQS2x5yNTYUEoIkO1VpSmTFu5fsFyoDgEkmNrCJpXtfM" --name prod-cluster

cluster delete
==============

This command deletes the specified cluster from the manager.
Note that there is no confirmation or warning to confirm.
If you deleted the cluster by mistake, you will need to add it again.

**Syntax:**

.. code-block:: none

   sctool cluster delete --cluster <id|name> [global flags]

.. note:: If you are removing the cluster from Scylla Manager and you are using Scylla Monitoring, remove the target from Prometheus Target list </operating-scylla/monitoring/monitoring_stack/#procedure>`_ in the prometheus/scylla_manager_servers.yml file.


cluster delete parameters
.........................

In addition to `Global flags`_, cluster delete takes the following parameter:

=====

.. include:: _common/param-cluster.rst

=====

Example: cluster delete
.......................

.. code-block:: none

   sctool cluster delete -c prod-cluster

.. _cluster-list:

cluster list
============

Lists the managed clusters.

**Syntax:**

.. code-block:: none

   sctool cluster list [global flags]

cluster list parameters
.......................

cluster list takes the `Global flags`_.

Example: cluster list
.....................

.. code-block:: none

   sctool cluster list
   ╭──────────────────────────────────────┬──────────────╮
   │ ID                                   │ Name         │
   ├──────────────────────────────────────┼──────────────┤
   │ db7faf98-7cc4-4a08-b707-2bc59d65551e │ prod-cluster │
   ╰──────────────────────────────────────┴──────────────╯

cluster update
==============

This command modifies managed cluster parameters.

**Syntax:**

.. code-block:: none

   sctool cluster update --cluster <id|name> [--host <node IP>] [--auth-token <token>] [--name <alias>] [--without-repair] [global flags]


cluster update parameters
.........................

In addition to the `Global flags`_, cluster update takes all the `cluster add parameters`_.

=====

.. include:: _common/param-cluster.rst

=====

.. include:: _common/cluster-params.rst

=====

Example: cluster update
.......................

In this example, the cluster named ``cluster`` has been renamed to ``prod-cluster``.

.. code-block:: none

   sctool cluster update --prod-cluster cluster --name prod-cluster

Backup cluster
--------------

The backup commands allow you to: create and update a backup (ad-hoc or scheduled), list the contents of a backup, and list the backups of a cluster.
You cannot initiate a backup without a cluster. Make sure you add a cluster (`cluster add`_) before initiating a backup.

.. code-block:: none

   sctool backup <subcommand> [global flags] [parameters]


**Commands**

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

In addition to the `Global flags`_, backup takes the following parameters:


=====

``--dc <list of glob patterns>``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

A comma-separated list of datacenter glob patterns, e.g. 'dc1,!otherdc*' used to specify the DCs to include or exclude from backup, separated by a comma.
This can also include glob patterns.

.. include:: _common/glob.rst

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

.. include:: _common/glob.rst

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

In addition to `Global flags`_, backup update takes the same parameters as `backup parameters`_

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

In addition to the `Global flags`_, backup list takes the following parameters:

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

.. include:: _common/glob.rst

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

In addition to the `Global flags`_, backup files add takes the following parameters:

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

.. include:: _common/glob.rst

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

In addition to the `Global flags`_, backup delete takes the following parameters:

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

Repair cluster
--------------

The repair commands allow you to: create and update a repair (ad-hoc or scheduled), and change selected parameters while a repair is running.

.. code-block:: none

   sctool repair <subcommand> [global flags] [parameters]

**Commands**

.. list-table::
   :widths: 30 70
   :header-rows: 1

   * - Command
     - Usage
   * - `repair`_
     - Schedule a repair (ad-hoc or scheduled).
   * - `repair control`_
     - Change parameters while a repair is running.
   * - `repair update`_
     - Modify properties of the existing repair task.

repair
======

The repair command allows you to schedule or run ad-hoc cluster repair.

.. code-block:: none

   sctool repair --cluster <id|name> [--dc <list of glob patterns>] [--dry-run]
   [--fail-fast] [--interval <time between task runs>]
   [--intensity <float>] [--keyspace <list of glob patterns>] [--parallel <integer>]
   [--start-date <now+duration|RFC3339>]
   [global flags]

repair parameters
.................

In addition to `Global flags`_, repair takes the following parameters:

=====

.. include:: _common/param-cluster.rst

=====

``--dc <list of glob patterns>``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

List of data centers to be repaired, separated by a comma.
This can also include glob patterns.

.. include:: _common/glob.rst

**Example**

Given the following data centers: *us-east-1*, *us-east-2*, *us-west-1*, *us-west-2*.

.. list-table::
   :widths: 50 50
   :header-rows: 1

   * - Parameter
     - Selects
   * - ``--dc us-east-1,us-west-2``
     - *us-east-1*, *us-west-2*
   * - ``--dc 'us-east-*'``
     - *us-east-1*, *us-east-2*
   * - ``--dc '*','!us-east-'``
     - *us-west-1*, *us-west-2*

**Default:** everything - all data centers

=====

``--dry-run``
^^^^^^^^^^^^^

Validates and displays repair information without actually scheduling the repair.
This allows you to display what will happen should the repair run with the parameters you set.

**Example**

Given the following keyspaces:

* system_auth
* system_distributed
* system_traces
* test_keyspace_dc1_rf2, test_keyspace_dc1_rf3, and test_keyspace_dc2_rf2
* keyspace_dc2_rf3
* test_keyspace_rf2 and test_keyspace_rf3

The following command will run a repair on all keyspaces **except** for test_keyspace_dc1_rf2 in dry-run mode.


.. code-block:: none

   sctool repair --dry-run -K '*,!test_keyspace_dc1_rf2'
   NOTICE: dry run mode, repair is not scheduled

   Data Centers:
     - dc1
     - dc2
   Keyspace: system_auth
     (all tables)
   Keyspace: system_distributed
     (all tables)
   Keyspace: system_traces
     (all tables)
   Keyspace: test_keyspace_dc1_rf3
     (all tables)
   Keyspace: test_keyspace_dc2_rf2
     (all tables)
   Keyspace: test_keyspace_dc2_rf3
     (all tables)
   Keyspace: test_keyspace_rf2
     (all tables)
   Keyspace: test_keyspace_rf3
     (all tables)

**Example with error**

.. code-block:: none

   sctool repair -K 'system*.bla' --dry-run -c bla
   NOTICE: dry run mode, repair is not scheduled

   Error: API error (status 400)
   {
     "message": "no matching units found for filters, ks=[system*.*bla*]",
     "trace_id": "b_mSOUoOSyqSnDtk9EANyg"
   }

=====

``--fail-fast``
^^^^^^^^^^^^^^^

Stops the repair process on the first error.

**Default:** False

=====

``--intensity <float>``
^^^^^^^^^^^^^^^^^^^^^^^

How many token ranges (per shard) to repair in a single Scylla repair job. By default this is 1.
If you set it to 0 the number of token ranges is adjusted to the maximum supported by node (see max_repair_ranges_in_parallel in Scylla logs).
Valid values are integers >= 1 and decimals between (0,1). Higher values will result in increased cluster load and slightly faster repairs.
Values below 1 will result in repairing the number of token ranges equal to the specified fraction of shards.
Changing the intensity impacts repair granularity if you need to resume it, the higher the value the more work on resume.

**Default:** 1

=====

``--parallel <integer>``
^^^^^^^^^^^^^^^^^^^^^^^^

The maximum number of Scylla repair jobs that can run at the same time (on different token ranges and replicas).
Each node can take part in at most one repair at any given moment. By default the maximum possible parallelism is used.
The effective parallelism depends on a keyspace replication factor (RF) and the nr. of nodes.
The formula to calculate is is as follows: nr. nodes / RF, ex. for 6 node cluster with RF=3 the maximum parallelism is 2.

**Default:** 0

=====

``-K, --keyspace <list of glob patterns>``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

A list of glob patterns separated by a comma.
The patterns match keyspaces and tables, when you write the pattern,
separate the keyspace name from the table name with a dot (*KEYSPACE.TABLE*).

.. include:: _common/glob.rst

**Example**

Given the following tables:

* *shopping_cart.cart*
* *orders.orders_by_date_2018_11_01*
* *orders.orders_by_date_2018_11_15*
* *orders.orders_by_date_2018_11_29*
* *orders.orders_by_date_2018_12_06*

.. list-table::
   :widths: 50 50
   :header-rows: 1

   * - Parameter
     - Selects
   * - ``-K '*'``
     - *everything - all tables in all keyspaces*
   * - ``-K shopping_cart``
     - *shopping_cart.cart*
   * - ``-K '*,!orders'``
     - *shopping_cart.cart*
   * - ``-K orders.'orders.2018_11_'``
     - *orders.orders_by_date_2018_11_01*
       *orders.orders_by_date_2018_11_15*
       *orders.orders_by_date_2018_11_29*
   * - ``-K 'orders.*2018_1?_2?'``
     - *orders.orders_by_date_2018_11_29*
   * - ``-K 'orders.*2018_11?[19]'``
     - *orders.orders_by_date_2018_11_01*
       *orders.orders_by_date_2018_11_29*

**Default:** everything - all tables in all keyspaces

=====

.. include:: _common/task-params.rst

=====

Example: Schedule a repair
..........................

Repairs can be scheduled to run on selected keyspaces/tables, nodes, or datacenters.
Scheduled repairs run every *n* days depending on the frequency you set.
A scheduled repair runs at the time you set it to run at.
If no time is given, the repair runs immediately.
Repairs can run once, or can run at a set schedule based on a time interval.

Repair cluster weekly
^^^^^^^^^^^^^^^^^^^^^

In this example, you create a repair task for a cluster named *prod-cluster*.
The task begins on May 2, 2019 at 3:04 PM.
It repeats every week at this time.
As there are no datacenters or keyspaces listed, all datacenters and all data in the specified cluster are repaired.

.. code-block:: none

   sctool repair -c prod-cluster -s 2019-05-02T15:04:05-07:00 --interval 7d

The system replies with a repair task ID.
You can use this ID to change the start time, stop the repair, or cancel the repair.

.. code-block:: none

   repair/3208ff15-6e8f-48b2-875c-d3c73f545410

Repair datacenters in a region weekly
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

This example repairs all datacenters starting with the name ``dc-asia-``, such as ``dc-asia-1``.
The repair begins on September 15, 2018 at 7:00 PM (JST, for example) and runs every week.

.. code-block:: none

   sctool repair -c prod-cluster --dc 'asia-*' -s 2018-09-15T19:00:05-07:00 --interval 7d

Repair selected keyspaces/tables weekly
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Using glob patterns gives you additional flexibility in selecting both keyspaces and tables.
This example repairs all tables in the *orders* keyspace starting with *2018_12_* prefix.
The repair is scheduled to run on December 4, 2018 at 8:00 AM and will run after that point every week.

.. code-block:: none

   sctool repair -c prod-cluster -K 'orders.2018_12_' -s 2018-12-04T08:00:05-07:00 --interval 7d

repair control
==============

The repair control command allows you to change repair parameters while a repair is running.

.. code-block:: none

   sctool repair control --cluster <id|name> [--intensity <float>] [--parallel <integer>]

repair control parameters
.........................

In addition to `Global flags`_, repair takes the following `repair parameters`_.

=====

``--intensity <float>``
^^^^^^^^^^^^^^^^^^^^^^^

How many token ranges (per shard) to repair in a single Scylla repair job. By default this is 1.
If you set it to 0 the number of token ranges is adjusted to the maximum supported by node (see max_repair_ranges_in_parallel in Scylla logs).
Valid values are integers >= 1 and decimals between (0,1). Higher values will result in increased cluster load and slightly faster repairs.
Values below 1 will result in repairing the number of token ranges equal to the specified fraction of shards.
Changing the intensity impacts repair granularity if you need to resume it, the higher the value the more work on resume.

**Default:** 1

=====

``--parallel <integer>``
^^^^^^^^^^^^^^^^^^^^^^^^

The maximum number of Scylla repair jobs that can run at the same time (on different token ranges and replicas).
Each node can take part in at most one repair at any given moment. By default the maximum possible parallelism is used.
The effective parallelism depends on a keyspace replication factor (RF) and the nr. of nodes.
The formula to calculate is is as follows: nr. nodes / RF, ex. for 6 node cluster with RF=3 the maximum parallelism is 2.

**Default:** 0

=====

repair update
=============

The repair update command allows you to modify properties of an already existing repair task.

.. code-block:: none

   sctool repair update <task_type/task_id> --cluster <id|name> [--dc <list of glob patterns>] [--dry-run]
   [--fail-fast] [--interval <time between task runs>]
   [--intensity <float>] [--keyspace <list of glob patterns>] [--parallel <integer>]
   [--start-date <now+duration|RFC3339>]
   [global flags]

repair update parameters
........................

In addition to `Global flags`_, repair update takes the same parameters as `repair parameters`_

Monitoring clusters
-------------------
.. _status:

status
======

The status command is an extended version of ``nodetool status``.
It can show status for all the managed clusters.
The first column shows node status in `nodetool format </operating-scylla/nodetool-commands/status/#nodetool-status>`_.
The CQL column shows the CQL status, SSL indicator if SSL is enabled on a node, and time the check took.

Available statuses are:

* UP - Situation normal
* DOWN - Failed to connect to host or CQL error
* UNAUTHORISED - Wrong username or password - only if ``username`` is specified for cluster
* TIMEOUT - Timeout

The REST column shows the status of Scylla Manager Server to Scylla API communication, and time the check took.

Available statuses are:

* UP - Situation normal
* DOWN - Failed to connect to host
* HTTP XXX - HTTP failure and its status code
* UNAUTHORISED - Wrong ``api-key`` specified for cluster or in `Scylla Manager Agent configuration file <../agent-configuration-file/#authentication-token>`_
* TIMEOUT - Timeout

The status information is also available as a metric in Scylla Monitoring Manager dashboard.
The `healthcheck` task checks nodes every 15 seconds, the interval can be changed using `task update`_ command.

**Syntax:**

.. code-block:: none

   sctool status [global flags]

status parameters
..................

status takes the `Global flags`_.

Example: status
................

.. code-block:: none

   sctool status -c prod-cluster
      Datacenter: eu-west
      ╭────┬────────────┬───────────┬───────────┬───────────────┬──────────┬──────┬──────────┬────────┬──────────┬──────────────────────────────────────╮
      │    │ Alternator │ CQL       │ REST      │ Address       │ Uptime   │ CPUs │ Memory   │ Scylla │ Agent    │ Host ID                              │
      ├────┼────────────┼───────────┼───────────┼───────────────┼──────────┼──────┼──────────┼────────┼──────────┼──────────────────────────────────────┤
      │ UN │ UP (4ms)   │ UP (3ms)  │ UP (2ms)  │ 34.203.122.52 │ 237h2m1s │ 4    │ 15.43GiB │ 4.1.0  │ 2.2.0    │ 8bfd18f1-ac3b-4694-bcba-30bc272554df │
      │ UN │ UP (15ms)  │ UP (11ms) │ UP (12ms) │ 10.0.138.46   │ 237h2m1s │ 4    │ 15.43GiB │ 4.1.0  │ 2.2.0    │ 238acd01-813c-4c55-bd65-5219bb19bc20 │
      │ UN │ UP (17ms)  │ UP (5ms)  │ UP (7ms)  │ 10.0.196.204  │ 237h2m1s │ 4    │ 15.43GiB │ 4.1.0  │ 2.2.0    │ bde4581a-b25e-49fc-8cd9-1651d7683f80 │
      │ UN │ UP (10ms)  │ UP (4ms)  │ UP (5ms)  │ 10.0.66.115   │ 237h2m1s │ 4    │ 15.43GiB │ 4.1.0  │ 2.2.0    │ 918a52aa-cc42-43a4-a499-f7b1ccb53b18 │
      ╰────┴────────────┴───────────┴───────────┴───────────────┴──────────┴──────┴──────────┴────────┴──────────┴──────────────────────────────────────╯

Managing tasks
--------------

The task command set allows you to schedule, start, stop and modify tasks.

.. code-block:: none

   sctool task <command> [flags] [global flags]

**Subcommands**

.. list-table::
   :widths: 30 70
   :header-rows: 1

   * - Subcommand
     - Usage
   * - `task delete`_
     - Delete a task.
   * - `task history`_
     - Show run history of a task.
   * - `task list`_
     - Show available tasks and their last run status.
   * - `task progress`_
     - Show the task progress.
   * - `task start`_
     - Start executing a task.
   * - `task stop`_
     - Stop executing a task.
   * - `task update`_
     - Modify a task.

task delete
===========

This command deletes a task from manager.
Note that a task can be disabled if you want to temporarily turn it off (see `task update`_).

**Syntax:**

.. code-block:: none

   sctool task delete <task type/id> --cluster <id|name> [global flags]

.. include:: _common/task-id-note.rst

task delete parameters
......................

In addition to the `Global Flags`_, task delete takes the following parameter:

=====

.. include:: _common/param-cluster.rst

=====

Example: task delete
....................

This example deletes the repair from the task list.
You need the task ID for this action.
This can be retrieved using the command ``sctool task list``.
Once the repair is removed, you cannot resume the repair.
You will have to create a new one.

.. code-block:: none

   sctool task delete -c prod-cluster repair/143d160f-e53c-4890-a9e7-149561376cfd


task history
============

This command shows a details about task run history for a give task.

**Syntax:**

.. code-block:: none

   sctool task history <task type/id> --cluster <id|name>
   [--limit <number of results>] [global flags]

.. include:: _common/task-id-note.rst

task history parameters
.......................

In addition to the `Global Flags`_, task history takes the following parameters:

=====

.. include:: _common/param-cluster.rst

=====

``--limit <number of results>``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Limits the number of returned results.

**Default** 10

=====

Example: task history
.....................

.. code-block:: none

   sctool task history repair/730a134a-4792-4139-bc6c-75d2ba7a1e62 -c prod-cluster

   ╭──────────────────────────────────────┬────────────────────────┬────────────────────────┬──────────┬────────────────────────────────────────────────╮
   │ ID                                   │ Start time             │ End time               │ Duration │ Status                                         │
   ├──────────────────────────────────────┼────────────────────────┼────────────────────────┼──────────┼────────────────────────────────────────────────┤
   │ f81ba8ad-ad79-11e8-915f-42010af000a9 │ 01 Jan 18 00:00:00 UTC │ 01 Jan 18 00:00:30 UTC │ 30s      │ STOPPED                                        │
   │ e02d2caf-ad2a-11e8-915e-42010af000a9 │ 31 Feb 18 14:33:05 UTC │ 31 Feb 18 14:34:35 UTC │ 90s      │ SUCCESS                                        │
   │ 7a8c6fe2-ad29-11e8-915d-42010af000a9 │ 31 Mar 18 14:23:20 UTC │ 31 Mar 18 14:23:20 UTC │ 0s       │ ERROR failed to load units …                   │
   │ 08f75324-610d-11e9-9aac-42010af000a9 │ 05 Apr 19 12:33:42 UTC │ 05 Apr 19 12:33:43 UTC │ 1s       │ DONE                                           │
   │ 000681e1-610d-11e9-9aab-42010af000a9 │ 09 Apr 19 12:33:27 UTC │ 09 Apr 19 12:33:28 UTC │ 1s       │ DONE                                           │
   │ f715fb82-610c-11e9-9aaa-42010af000a9 │ 11 Apr 19 12:33:12 UTC │ 11 Apr 19 12:33:13 UTC │ 1s       │ DONE                                           │
   │ ee251fc0-610c-11e9-9aa9-42010af000a9 │ 13 Apr 19 12:32:57 UTC │ 13 Apr 19 12:32:58 UTC │ 1s       │ DONE                                           │
   │ e5343b52-610c-11e9-9aa8-42010af000a9 │ 15 Apr 19 15:32:42 UTC │ 15 Apr 19 15:32:43 UTC │ 1s       │ DONE                                           │
   │ dc435562-610c-11e9-9aa7-42010af000a9 │ 17 Apr 19 12:32:27 UTC │ 17 Apr 19 12:32:28 UTC │ 1s       │ DONE                                           │
   ╰──────────────────────────────────────┴────────────────────────┴────────────────────────┴──────────┴────────────────────────────────────────────────╯
.. _task-list:

task list
=========

This command shows all of the scheduled tasks for the specified cluster.
If cluster is not set this would output a table for every cluster.
Each row contains task type and ID, separated by a slash, task properties, next activation and last status information.
For more information on a task consult `task history`_ and `task progress`_.

**Syntax:**

.. code-block:: none

   sctool task list [--cluster <id|name>] [--all] [--sort <sort-key>]
   [--status <status>] [--type <task type>] [global flags]

task list parameters
....................

In addition to the `Global Flags`_, task list takes the following parameters:

=====

.. include:: _common/param-cluster.rst

=====

``--all``
^^^^^^^^^

Lists all tasks, including those which have been disabled.
Disabled tasks are prefixed with ``*``.
For example ``*repair/afe9a610-e4c7-4d05-860e-5a0ddf14d7aa``.

=====

``--sort <sort-key>``
^^^^^^^^^^^^^^^^^^^^^

Returns a list of tasks sorted according to the last run status and sort key which you provide.
Accepted sort key values are:

* ``start-time``
* ``next-activation``
* ``end-time``
* ``status``

``start-time``, ``next-activation``, and ``end-time`` are sorted in ascending order.

``status`` is sorted using the following order: "NEW", "RUNNING", "STOPPED", "DONE", "ERROR", "ABORTED".

=====

``--status <status>``
^^^^^^^^^^^^^^^^^^^^^

Filters tasks according to their last run status.
Accepted values are NEW, STARTING, RUNNING, STOPPING, STOPPED, DONE, ERROR, ABORTED.

=====

``-t, --type <task type>``
^^^^^^^^^^^^^^^^^^^^^^^^^^

Display only tasks of a given type.

=====

Example: task list
..................

.. code-block:: none

   sctool task list
   Cluster: prod-cluster (c1bbabf3-cad1-4a59-ab8f-84e2a73b623f)
   ╭───────────────────────────────────────────────────────┬──────────────────────────────────────┬───────────────────────────────┬─────────╮
   │ Task                                                  │ Arguments                            │ Next run                      │ Status  │
   ├───────────────────────────────────────────────────────┼──────────────────────────────────────┼───────────────────────────────┼─────────┤
   │ healthcheck/ebccaade-4487-4ce9-80ee-d39e295c752e      │                                      │ 02 Apr 20 10:47:48 UTC (+15s) │ DONE    │
   │ healthcheck_rest/eff03af0-21ee-473a-b674-5e4bedc37b8b │                                      │ 02 Apr 20 11:02:03 UTC (+1h)  │ DONE    │
   │ backup/2d7df8bb-69bc-4782-a52f-1ec87f9c2c1c           │ -L s3:manager-backup-tests-eu-west-1 │                               │ RUNNING │
   ╰───────────────────────────────────────────────────────┴──────────────────────────────────────┴───────────────────────────────┴─────────╯

Setting the ``--all`` flag will also list disabled tasks which are not shown in the regular view.
Disabled tasks are prefixed with a ``*``.

task progress
=============

This command shows details of the latest run (or still running) task.

**Syntax:**

.. code-block:: none

   sctool task progress <task type/id> --cluster <id|name> [--details] [global flags]

.. include:: _common/task-id-note.rst

task progress parameters
........................


In addition to the `Global flags`_, repair progress takes the following parameters:

=====

.. include:: _common/param-cluster.rst

=====

``--details``
^^^^^^^^^^^^^

More detailed progress data, depending on task type.

====

Example: task progress
......................

This example displays the progress of a running repair.

.. code-block:: none

   sctool task progress repair/dff91fd1-f430-4f98-8932-373644fe647e -c prod-cluster
   Status:         RUNNING
   Start time:     17 Apr 19 12:55:57 UTC
   Duration:       46s
   Progress:       0.45%
   Datacenters:
     - dc1
     - dc2
   ╭───────────────────────┬───────╮
   │ system_auth           │ 3.85% │
   │ system_distributed    │ 0.00% │
   │ system_traces         │ 0.00% │
   │ test_keyspace_dc1_rf2 │ 0.00% │
   │ test_keyspace_dc1_rf3 │ 0.00% │
   │ test_keyspace_dc2_rf2 │ 0.00% │
   │ test_keyspace_dc2_rf3 │ 0.00% │
   │ test_keyspace_rf2     │ 0.00% │
   │ test_keyspace_rf3     │ 0.00% │
   ╰───────────────────────┴───────╯

The ``--details`` flag shows each host’s shard repair progress, with the shards numbered from zero.

.. code-block:: none

   sctool task progress repair/dff91fd1-f430-4f98-8932-373644fe647e -c prod-cluster --details
   Status:         RUNNING
   Start time:     17 Apr 19 12:55:57 UTC
   Duration:       3m0s
   Progress:       1.91%
   Datacenters:
     - dc1
     - dc2
   ╭───────────────────────┬────────╮
   │ system_auth           │ 16.30% │
   │ system_distributed    │ 0.00%  │
   │ system_traces         │ 0.00%  │
   │ test_keyspace_dc1_rf2 │ 0.00%  │
   │ test_keyspace_dc1_rf3 │ 0.00%  │
   │ test_keyspace_dc2_rf2 │ 0.00%  │
   │ test_keyspace_dc2_rf3 │ 0.00%  │
   │ test_keyspace_rf2     │ 0.00%  │
   │ test_keyspace_rf3     │ 0.00%  │
   ╰───────────────────────┴────────╯
   ╭───────────────────────┬───────┬──────────┬───────────────┬─────────────────┬───────────────╮
   │ system_auth           │ shard │ progress │ segment_count │ segment_success │ segment_error │
   ├───────────────────────┼───────┼──────────┼───────────────┼─────────────────┼───────────────┤
   │ 192.168.100.11        │ 0     │ 100.00%  │ 748           │ 748             │ 0             │
   │ 192.168.100.11        │ 1     │ 100.00%  │ 757           │ 757             │ 0             │
   │ 192.168.100.22        │ 0     │ 2.40%    │ 791           │ 19              │ 0             │
   │ 192.168.100.22        │ 1     │ 2.35%    │ 807           │ 19              │ 0             │
   │ 192.168.100.12        │ 0     │ 0.00%    │ 740           │ 0               │ 0             │
   │ 192.168.100.12        │ 1     │ 0.00%    │ 740           │ 0               │ 0             │
   │ 192.168.100.13        │ 0     │ 0.00%    │ 922           │ 0               │ 0             │
   │ 192.168.100.13        │ 1     │ 0.00%    │ 930           │ 0               │ 0             │
   │ 192.168.100.21        │ 0     │ 0.00%    │ 765           │ 0               │ 0             │
   │ 192.168.100.21        │ 1     │ 0.00%    │ 767           │ 0               │ 0             │
   │ 192.168.100.23        │ 0     │ 0.00%    │ 752           │ 0               │ 0             │
   │ 192.168.100.23        │ 1     │ 0.00%    │ 747           │ 0               │ 0             │
   ├───────────────────────┼───────┼──────────┼───────────────┼─────────────────┼───────────────┤
   │ system_distributed    │ shard │ progress │ segment_count │ segment_success │ segment_error │
   ├───────────────────────┼───────┼──────────┼───────────────┼─────────────────┼───────────────┤
   │ 192.168.100.11        │ 0     │ 0.00%    │ 748           │ 0               │ 0             │
   │ 192.168.100.11        │ 1     │ 0.00%    │ 757           │ 0               │ 0             │
   │ 192.168.100.12        │ 0     │ 0.00%    │ 740           │ 0               │ 0             │
   │ 192.168.100.12        │ 1     │ 0.00%    │ 740           │ 0               │ 0             │
   │ 192.168.100.13        │ 0     │ 0.00%    │ 922           │ 0               │ 0             │
   │ 192.168.100.13        │ 1     │ 0.00%    │ 930           │ 0               │ 0             │
   │ 192.168.100.21        │ 0     │ 0.00%    │ 765           │ 0               │ 0             │
   │ 192.168.100.21        │ 1     │ 0.00%    │ 767           │ 0               │ 0             │
   │ 192.168.100.22        │ 0     │ 0.00%    │ 791           │ 0               │ 0             │
   ├───────────────────────┼───────┼──────────┼───────────────┼─────────────────┼───────────────┤
   │ system_traces         │ shard │ progress │ segment_count │ segment_success │ segment_error │
   ├───────────────────────┼───────┼──────────┼───────────────┼─────────────────┼───────────────┤
   │ 192.168.100.11        │ 0     │ 0.00%    │ 748           │ 0               │ 0             │
   │ 192.168.100.11        │ 1     │ 0.00%    │ 757           │ 0               │ 0             │
   │ 192.168.100.12        │ 0     │ 0.00%    │ 740           │ 0               │ 0             │
   │ 192.168.100.12        │ 1     │ 0.00%    │ 740           │ 0               │ 0             │
   │ 192.168.100.13        │ 0     │ 0.00%    │ 922           │ 0               │ 0             │
   │ 192.168.100.13        │ 1     │ 0.00%    │ 930           │ 0               │ 0             │
   │ 192.168.100.21        │ 0     │ 0.00%    │ 765           │ 0               │ 0             │
   │ 192.168.100.21        │ 1     │ 0.00%    │ 767           │ 0               │ 0             │
   │ 192.168.100.22        │ 0     │ 0.00%    │ 791           │ 0               │ 0             │
   │ 192.168.100.22        │ 1     │ 0.00%    │ 807           │ 0               │ 0             │
   │ 192.168.100.23        │ 0     │ 0.00%    │ 752           │ 0               │ 0             │
   │ 192.168.100.23        │ 1     │ 0.00%    │ 747           │ 0               │ 0             │
   ├───────────────────────┼───────┼──────────┼───────────────┼─────────────────┼───────────────┤
   │ test_keyspace_dc1_rf2 │ shard │ progress │ segment_count │ segment_success │ segment_error │
   ├───────────────────────┼───────┼──────────┼───────────────┼─────────────────┼───────────────┤
   │ 192.168.100.11        │ 0     │ 0.00%    │ 1339          │ 0               │ 0             │
   │ 192.168.100.11        │ 1     │ 0.00%    │ 1339          │ 0               │ 0             │
   │ 192.168.100.12        │ 0     │ 0.00%    │ 1482          │ 0               │ 0             │
   │ 192.168.100.12        │ 1     │ 0.00%    │ 1480          │ 0               │ 0             │
   │ 192.168.100.13        │ 0     │ 0.00%    │ 1523          │ 0               │ 0             │
   │ 192.168.100.13        │ 1     │ 0.00%    │ 1528          │ 0               │ 0             │
   ├───────────────────────┼───────┼──────────┼───────────────┼─────────────────┼───────────────┤
   │ test_keyspace_dc1_rf3 │ shard │ progress │ segment_count │ segment_success │ segment_error │
   ├───────────────────────┼───────┼──────────┼───────────────┼─────────────────┼───────────────┤
   │ 192.168.100.11        │ 0     │ 0.00%    │ 1339          │ 0               │ 0             │
   │ 192.168.100.11        │ 1     │ 0.00%    │ 1339          │ 0               │ 0             │
   │ 192.168.100.12        │ 0     │ 0.00%    │ 1482          │ 0               │ 0             │
   │ 192.168.100.12        │ 1     │ 0.00%    │ 1480          │ 0               │ 0             │
   │ 192.168.100.13        │ 0     │ 0.00%    │ 1523          │ 0               │ 0             │
   │ 192.168.100.13        │ 1     │ 0.00%    │ 1528          │ 0               │ 0             │
   ├───────────────────────┼───────┼──────────┼───────────────┼─────────────────┼───────────────┤
   │ test_keyspace_dc2_rf2 │ shard │ progress │ segment_count │ segment_success │ segment_error │
   ├───────────────────────┼───────┼──────────┼───────────────┼─────────────────┼───────────────┤
   │ 192.168.100.21        │ 0     │ 0.00%    │ 1349          │ 0               │ 0             │
   │ 192.168.100.21        │ 1     │ 0.00%    │ 1343          │ 0               │ 0             │
   │ 192.168.100.22        │ 0     │ 0.00%    │ 1550          │ 0               │ 0             │
   │ 192.168.100.22        │ 1     │ 0.00%    │ 1561          │ 0               │ 0             │
   │ 192.168.100.23        │ 0     │ 0.00%    │ 1450          │ 0               │ 0             │
   │ 192.168.100.23        │ 1     │ 0.00%    │ 1465          │ 0               │ 0             │
   ├───────────────────────┼───────┼──────────┼───────────────┼─────────────────┼───────────────┤
   │ test_keyspace_dc2_rf3 │ shard │ progress │ segment_count │ segment_success │ segment_error │
   ├───────────────────────┼───────┼──────────┼───────────────┼─────────────────┼───────────────┤
   │ 192.168.100.21        │ 0     │ 0.00%    │ 1349          │ 0               │ 0             │
   │ 192.168.100.21        │ 1     │ 0.00%    │ 1343          │ 0               │ 0             │
   │ 192.168.100.22        │ 0     │ 0.00%    │ 1550          │ 0               │ 0             │
   │ 192.168.100.22        │ 1     │ 0.00%    │ 1561          │ 0               │ 0             │
   │ 192.168.100.23        │ 0     │ 0.00%    │ 1450          │ 0               │ 0             │
   │ 192.168.100.23        │ 1     │ 0.00%    │ 1465          │ 0               │ 0             │
   ├───────────────────────┼───────┼──────────┼───────────────┼─────────────────┼───────────────┤
   │ test_keyspace_rf2     │ shard │ progress │ segment_count │ segment_success │ segment_error │
   ├───────────────────────┼───────┼──────────┼───────────────┼─────────────────┼───────────────┤
   │ 192.168.100.21        │ 0     │ 0.00%    │ 1349          │ 0               │ 0             │
   │ 192.168.100.21        │ 1     │ 0.00%    │ 1343          │ 0               │ 0             │
   │ 192.168.100.22        │ 0     │ 0.00%    │ 1550          │ 0               │ 0             │
   │ 192.168.100.22        │ 1     │ 0.00%    │ 1561          │ 0               │ 0             │
   │ 192.168.100.23        │ 0     │ 0.00%    │ 1450          │ 0               │ 0             │
   │ 192.168.100.23        │ 1     │ 0.00%    │ 1465          │ 0               │ 0             │
   ├───────────────────────┼───────┼──────────┼───────────────┼─────────────────┼───────────────┤
   │ test_keyspace_rf3     │ shard │ progress │ segment_count │ segment_success │ segment_error │
   ├───────────────────────┼───────┼──────────┼───────────────┼─────────────────┼───────────────┤
   │ 192.168.100.11        │ 0     │ 0.00%    │ 1339          │ 0               │ 0             │
   │ 192.168.100.11        │ 1     │ 0.00%    │ 1339          │ 0               │ 0             │
   │ 192.168.100.12        │ 0     │ 0.00%    │ 1482          │ 0               │ 0             │
   │ 192.168.100.12        │ 1     │ 0.00%    │ 1480          │ 0               │ 0             │
   │ 192.168.100.13        │ 0     │ 0.00%    │ 1523          │ 0               │ 0             │
   │ 192.168.100.13        │ 1     │ 0.00%    │ 1528          │ 0               │ 0             │
   ╰───────────────────────┴───────┴──────────┴───────────────┴─────────────────┴───────────────╯

Example with a backup task

.. code-block:: none

   sctool task progress -c prod-cluster backup/f0642e3e-e1cb-44ba-8447-f8d43672bcfd
   Arguments:      -L s3:manager-backup-tests-eu-west-1
   Status:         RUNNING
   Start time:     02 Apr 20 10:09:27 UTC
   Duration:       40m30s
   Progress:       1%
   Snapshot Tag:   sm_20200402100931UTC
   Datacenters:
   - eu-west
   ╭──────────────┬──────────┬───────────┬──────────┬──────────────┬────────╮
   │ Host         │ Progress │      Size │  Success │ Deduplicated │ Failed │
   ├──────────────┼──────────┼───────────┼──────────┼──────────────┼────────┤
   │ 10.0.114.68  │       1% │ 952.11GiB │ 13.22GiB │       538KiB │     0B │
   │ 10.0.138.46  │       1% │ 938.00GiB │ 13.43GiB │       830MiB │     0B │
   │ 10.0.196.204 │       1% │ 934.58GiB │ 13.79GiB │       206MiB │     0B │
   │ 10.0.66.115  │       1% │ 897.43GiB │ 12.17GiB │       523KiB │     0B │
   ╰──────────────┴──────────┴───────────┴──────────┴──────────────┴────────╯

task start
==========

This command initiates a task run.
Note that if a repair task is already running on a cluster, other repair tasks runs on that cluster will fail.

**Syntax:**

.. code-block:: none

   sctool task start <task type/id> --cluster <id|name> [global flags]

.. include:: _common/task-id-note.rst

task start parameters
.....................

In addition to the `Global Flags`_, task start takes the following parameters:

=====

.. include:: _common/param-cluster.rst

=====

``--continue``
^^^^^^^^^^^^^^

Try to resume the last run.

**Default** true

=====

Example: task start
...................

This example resumes running which was previously stopped.
To start a repair which is scheduled, but is currently not running use the ``task update`` command making sure to set the start time to ``now``.
See `Example: task update`_.


If you have stopped a repair you can resume it by running the following command.
You will need the task ID for this action.
This can be retrieved using the command ``sctool task list``.

.. code-block:: none

   sctool task start -c prod-cluster repair/143d160f-e53c-4890-a9e7-149561376cfd

task stop
=========

Stops a specified task, stopping an already stopped task has no effect.

**Syntax:**

.. code-block:: none

   sctool task stop <task type/id> --cluster <id|name> [global flags]

.. include:: _common/task-id-note.rst

task stop parameters
.....................

In addition to the `Global flags`_, task stop takes the following parameter:

=====

.. include:: _common/param-cluster.rst

=====

Example: task stop
..................

This example immediately stops a running repair.
The task is not deleted and can be resumed at a later time.
You will need the task ID for this action.
This can be retrieved using the command ``sctool task list``.

.. code-block:: none

   sctool task stop -c prod-cluster repair/143d160f-e53c-4890-a9e7-149561376cfd

task update
===========

This command changes generic task parameters such as schedule.

**Syntax:**

.. code-block:: none

   sctool task update <task type/id> --cluster <id|name> [--enabled <bool>]
   [--name <alias>] [--tags <list of tags>]
   [--interval <time between task runs>]
   [--start-date <now+duration|RFC3339>]
   [global flags]

.. include:: _common/task-id-note.rst

task update parameters
......................

In addition to `Global flags`_, task stop takes the following parameters:

=====

.. include:: _common/param-cluster.rst

=====

``-e, --enabled``
^^^^^^^^^^^^^^^^^

Setting enabled to false disables the task.
Disabled tasks are not executed and hidden from task list.
To show disabled tasks invoke ``sctool task list --all`` (see `task list`_).

**Default** true

=====

``-n, --name <alias>``
^^^^^^^^^^^^^^^^^^^^^^

Adds a name to a task.

=====

``--tags <list of tags>``
^^^^^^^^^^^^^^^^^^^^^^^^^

Allows you to tag the task with a list of text.

=====

.. include:: _common/task-params.rst

=====

Example: task update
....................

This example disables the task

.. code-block:: none

   sctool task update -c prod-cluster repair/4d79ee63-7721-4105-8c6a-5b98c65c3e21 --enabled false


This example reschedules the repair to run in 3 hours from now instead of whatever time it was supposed to run and sets the repair to run every two days.
The new time you set replaces the time which was previously set.

.. code-block:: none

   sctool task update -c prod-cluster repair/143d160f-e53c-4890-a9e7-149561376cfd -s now+3h --interval 2d

Show Scylla Manager version
---------------------------

This command shows the currently installed sctool version and the Scylla Manager server version.

version
=======

**Syntax:**

.. code-block:: none

   sctool version [global flags]

version parameters
..................

version takes the `Global flags`_.

Example: version
................

.. code-block:: none

   sctool version
   Client version: 2.1-0.20200401.ce91f2ad
   Server version: 2.1-0.20200401.ce91f2ad
