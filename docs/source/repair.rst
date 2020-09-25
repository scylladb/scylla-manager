======
Repair
======

.. note:: If, after upgrading to the latest Scylla, you experience repairs that are slower than usual please consider `upgrading Scylla Manager to the appropriate version </upgrade/upgrade-manager/upgrade-guide-from-2.x.a-to-2.y.b/upgrade-row-level-repair>`_.

When you create a cluster a repair job is automatically scheduled. 
This task is set to occur each week by default, but you can change it to another time, or add additional repair tasks. 
It is important to make sure that data across the nodes is consistent when maintaining your clusters.

.. contents::
   :depth: 3
   :local:

Why repair with Scylla Manager
-------------------------------

Scylla Manager automates the repair process and allows you to manage how and when the repair occurs. 
The advantage of repairing the cluster with Scylla Manager is:

* Scylla versions can defer in features they support so Scylla Manager detects and selects most efficient algorithm for repair based on available Scylla node version.

* By default it handles latest row-level repair available in newer Scylla versions.

* For Scylla nodes that require legacy repair clusters are repaired node by node, ensuring that each database shard performs exactly one repair task at a time.
  This gives the best repair parallelism on a node, shortens the overall repair time, and does not introduce unnecessary load.

* If there is an error, Scylla Manager’s retry mechanism will try to run the repair again.

* It has a restart (pause) mechanism that allows for restarting a repair from where it left off.

* Repair what you want, when you want, and how often you want. Manager gives you that flexibility.

* The most apparent advantage is that with Manager you do not have to manually SSH into every node as you do with nodetool.

What can you repair with Scylla Manager
----------------------------------------

Scylla Manager can repair any item which it manages, specifically:

* Specific tables, keyspaces, clusters, or data centers.

* A group of tables, keyspaces, clusters or data centers.

* All tables, keyspaces, clusters, or data centers.


What sort of repairs can I run with Scylla Manager
---------------------------------------------------

You can run two types of repairs:

* Ad-hoc - this is a one time repair 

* Scheduled - this repair is scheduled in advance and can repeat

Schedule a Repair
-----------------

By default, a cluster successfully added to Scylla Manager has a repair task created for it which repairs the entire cluster. 
This is a repeating task which runs every week. 
You can change this repair, add additional repairs, or delete this repair. 
You can schedule repairs to run in the future on a regular basis, schedule repairs to run once, or schedule repairs to run immediately on an as needed basis. 
Any repair can be rescheduled, paused, resumed, or deleted. 
For information on what is repaired and the types of repairs available see `What can you repair with Scylla Manager`_. 

Create a scheduled repair
.........................

While the most recommended way to run a repair is across an entire cluster, repairs can be scheduled to run on a single/multiple datacenters, keyspaces, or tables.
Scheduled repairs run every X days depending on the frequency you set. 
The procedure here shows the most frequently used repair command. 
Additional parameters are located in the `sctool Reference <../sctool/#repair-parameters>`_.

**Procedure**

Run the following sctoool repair command, replacing the parameters with your own parameters:

* ``-c`` - cluster name - replace `prod-cluster` with the name of your cluster

* ``-s`` - start-time - replace 2018-01-02T15:04:05-07:00 with the time  you want the repair to begin

* ``-i`` - interval - replace -i 7d with your own time interval

For example:

.. code-block:: none

   sctool repair -c prod-cluster -s 2018-01-02T15:04:05-07:00 -i 7d

2. The command returns the task ID. You will need this ID for additional actions.

3. If you want to run the repair only once, remove the `-i` argument. 

4. If you want to run this command immediately, but still want the repair to repeat, keep the interval argument (``-i``), but remove the start-date (``-s``).

Schedule an ad-hoc repair
.........................

An ad-hoc repair runs immediately and does not repeat. 
This procedure shows the most frequently used repair command. 
Additional parameters can be used. Refer to the `sctool Reference <../sctool/#repair-parameters>`_.

**Procedure**

1. Run the following command, replacing the -c argument with your cluster name: 

.. code-block:: none

   sctool repair -c prod-cluster

2. The command returns the task ID. You will need this ID for additional actions.

Monitor progress of the repair task
...................................

Progress of the repair task can be monitored by using `sctool task progress <../sctool/#task-progress>`_ command and providing UUID of the repair task.

.. code-block:: none

   sctool task progress repair/143d160f-e53c-4890-a9e7-149561376cfd -c prod-cluster

Repair faster or slower
.......................

When scheduling repair tasks, you may specify the following flags to influence repair speed and load on the cluster.

*``--intensity``
* ``--parallel``
When using these flags, note the following:

* ``--intensity``  must be either an integer >= 1, or a decimal between (0-1).
* ``--intensity`` value will be translated into the number of segments repaired by Scylla in a single repair command. Higher values result in higher speed and may increase cluster load.
* ``--parallel`` is an integer number that sets limit of repair commands that can run in parallel.
* Parallel will be capped at maximum value calculated at runtime from the number of disjoint groups of replica.

Please note that this only applies to clusters where row-level repair is available. When supporting legacy repair Scylla Manager will split segments into shards so total amount of requests will be greater but the same base algorithm for ``--intensity``  and ``--parallel`` will be respected.

**Example**

.. code-block:: none

   # This repair task will repair 16 segments in a single repair command
   sctool repair -c prod-cluster --intensity 16

Reschedule a Repair
-------------------

You can change the run time of a scheduled repair using the update repair command. 
The new time you set replaces the time which was previously set. 
This command requires the task ID which was generated when you set the repair. 
This can be retrieved using the command sctool `task list <../sctool/#task-list>`_.

This example updates a task to run in 3 hours instead of whatever time it was supposed to run.

.. code-block:: none

   sctool task update -c prod-cluster repair/143d160f-e53c-4890-a9e7-149561376cfd -s now+3h

To start a scheduled repair immediately, run the following command inserting the task id and cluster name:

.. code-block:: none

   sctool task start repair/143d160f-e53c-4890-a9e7-149561376cfd -c prod-cluster

Control a Repair speed at runtime
---------------------------------

To change execution properties of the running repair, use the command ``sctool repair control``.

.. code-block:: none

   sctool repair control  -c prod-cluster --intensity 0.5 --parallel 3


Pause a Repair
--------------

Pauses a specified task, provided it is running. 
You will need the task ID for this action. 
This can be retrieved using the command ``sctool task list``. To start the task again see `Resume a Repair`_.
 
.. code-block:: none
 
   sctool task stop repair/143d160f-e53c-4890-a9e7-149561376cfd -c prod-cluster

Resume a Repair 
---------------

Re-start a repair that is currently in the paused state. 
To start running a repair which is scheduled, but is currently not running, use the task update command. 
See `Reschedule a Repair`_.
You will need the task ID for this action. This can be retrieved using the command ``sctool task list``.

.. code-block:: none

   sctool task start repair/143d160f-e53c-4890-a9e7-149561376cfd -c prod-cluster

Delete a Repair
---------------

This action removes the repair from the task list. 
Once removed, you cannot resume the repair. 
You will have to create a new one.  
You will need the task ID for this action. 
This can be retrieved using the command ``sctool task list``.

.. code-block:: none

   sctool task delete repair/143d160f-e53c-4890-a9e7-149561376cfd -c prod-cluster
