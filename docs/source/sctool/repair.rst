.. _repair-commands:

Repair
------

The repair commands allow you to: create and update a repair (ad-hoc or scheduled), and change selected parameters while a repair is running.

.. code-block:: none

   sctool repair <subcommand> [global flags] [parameters]

**Subcommands**

.. list-table::
   :widths: 30 70
   :header-rows: 1

   * - Command
     - Usage
   * - :ref:`sctool-repair`
     - Schedule a repair (ad-hoc or scheduled).
   * - :ref:`repair-control`
     - Change parameters while a repair is running.
   * - :ref:`repair-update`
     - Modify properties of the existing repair task.

.. _sctool-repair:

repair
======

The repair command allows you to schedule or run ad-hoc cluster repair.

.. code-block:: none

   sctool repair --cluster <id|name> [--dc <list of glob patterns>] [--dry-run]
   [--fail-fast] [--interval <time between task runs>] [--host <node IP>]
   [--intensity <float>] [--keyspace <list of glob patterns>] [--parallel <integer>]
   [--start-date <now+duration|RFC3339>]
   [global flags]

.. _repair-parameters:

repair parameters
.................

In addition to :ref:`global-flags`, repair takes the following parameters:

=====

.. include:: ../_common/param-cluster.rst

=====

``--dc <list of glob patterns>``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

List of data centers to be repaired, separated by a comma.
This can also include glob patterns.

.. include:: ../_common/glob.rst

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

``--host <node IP>``
^^^^^^^^^^^^^^^^^^^^

Address of a node to repair, you can use either an IPv4 or IPv6 address.
Specifying the host flag limits repair to token ranges replicated by a given node.
It can be used in conjunction with dc flag, in such a case the node must belong to the specified datacenters.

=====

``--intensity <float>``
^^^^^^^^^^^^^^^^^^^^^^^

How many token ranges (per shard) to repair in a single Scylla repair job. By default this is 1.
If you set it to 0 the number of token ranges is adjusted to the maximum supported by node (see max_repair_ranges_in_parallel in Scylla logs).
Valid values are 0 and integers >= 1.
Higher values will result in increased cluster load and slightly faster repairs.
Changing the intensity impacts repair granularity if you need to resume it, the higher the value the more work on resume.

.. note::
   For Scylla clusters that **do not support row-level repair**, intensity can be a decimal between (0,1).
   In that case it specifies percent of shards that can be repaired in parallel on a repair master node.
   For Scylla clusters that are row-level repair enabled, setting intensity below 1 has the same effect as setting intensity 1.

**Default:** 1

=====

``--parallel <integer>``
^^^^^^^^^^^^^^^^^^^^^^^^

The maximum number of Scylla repair jobs that can run at the same time (on different token ranges and replicas).
Each node can take part in at most one repair at any given moment. By default the maximum possible parallelism is used.
The effective parallelism depends on a keyspace replication factor (RF) and the number of nodes.
The formula to calculate is is as follows: number of nodes / RF, ex. for 6 node cluster with RF=3 the maximum parallelism is 2.

**Default:** 0

=====

``-K, --keyspace <list of glob patterns>``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

A list of glob patterns separated by a comma.
The patterns match keyspaces and tables, when you write the pattern,
separate the keyspace name from the table name with a dot (*KEYSPACE.TABLE*).

.. include:: ../_common/glob.rst

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

.. include:: ../_common/task-params.rst

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

Repair a specific keyspace or table weekly
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Using glob patterns gives you additional flexibility in selecting both keyspaces and tables.
This example repairs all tables in the *orders* keyspace starting with *2018_12_* prefix.
The repair is scheduled to run on December 4, 2018 at 8:00 AM and will run after that point every week.

.. code-block:: none

   sctool repair -c prod-cluster -K 'orders.2018_12_' -s 2018-12-04T08:00:05-07:00 --interval 7d

Repair a specific node
^^^^^^^^^^^^^^^^^^^^^^

You can limit scope of repair to token ranges replicated by a specific node by specifying ``--host`` flag.
This is equivalent to running ``nodetool repair -full`` on that node.
If you want to recreate a node in a multi DC cluster, you can repair with local datacenter only.
To do that you must specify the ``--dc`` flag pointing to the datacenter where the node belongs.

This example, repairs node with IP ``34.203.122.52`` that belongs to datacenter named ``eu-west`` within that datacenter.

.. code-block:: none

   sctool repair -c prod-cluster --host 34.203.122.52 --dc eu-west


.. _repair-control:

repair control
==============

The repair control command allows you to change repair parameters while a repair is running.

.. code-block:: none

   sctool repair control --cluster <id|name> [--intensity <float>] [--parallel <integer>]

repair control parameters
.........................

In addition to :ref:`global-flags`, repair takes the following repair control parameters:

=====

.. _intensity-float:

``--intensity <float>``
^^^^^^^^^^^^^^^^^^^^^^^

How many token ranges (per shard) to repair in a single Scylla repair job. By default this is 1.
If you set it to 0 the number of token ranges is adjusted to the maximum supported by node (see max_repair_ranges_in_parallel in Scylla logs).
Valid values are integers >= 1 and decimals between (0,1). Higher values will result in increased cluster load and slightly faster repairs.
Values below 1 will result in repairing the number of token ranges equal to the specified fraction of shards.
Changing the intensity impacts repair granularity if you need to resume it, the higher the value the more work on resume.

**Default:** 1

For examples, see :ref:`Change Repair Speed <change-speed>`.

=====

.. _parallel-integer:

``--parallel <integer>``
^^^^^^^^^^^^^^^^^^^^^^^^

The maximum number of Scylla repair jobs that can run at the same time (on different token ranges and replicas).
Each node can take part in at most one repair at any given moment. By default the maximum possible parallelism is used.
The effective parallelism depends on a keyspace replication factor (RF) and the nr. of nodes.
The formula to calculate is is as follows: nr. nodes / RF, ex. for 6 node cluster with RF=3 the maximum parallelism is 2.

**Default:** 0

For examples, see :ref:`Change Repair Speed <change-speed>`.

=====

.. _repair-update:
.. _reschedule-a-repair:

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

In addition to :ref:`global-flags`, repair update takes the same parameters as `repair parameters`_

repair update example
.....................

This example updates the repair task 143d160f-e53c-4890-a9e7-149561376cfd adding an intensity parameter to speed up the repair.

.. code-block:: none

   sctool repair update -c prod-cluster repair/143d160f-e53c-4890-a9e7-149561376cfd --intensity 0