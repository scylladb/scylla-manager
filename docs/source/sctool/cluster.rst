Cluster
-------

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

In addition to the :ref:`global-flags`, cluster add takes the following parameters:

=====

.. include:: ../_common/cluster-params.rst

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

In addition to :ref:`global-flags`, cluster delete takes the following parameter:

=====

.. include:: ../_common/param-cluster.rst

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

cluster list takes the :ref:`global-flags`.

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

In addition to the :ref:`global-flags`, cluster update takes all the `cluster add parameters`_.

=====

.. include:: ../_common/param-cluster.rst

=====

.. include:: ../_common/cluster-params.rst

=====

Example: cluster update
.......................

In this example, the cluster named ``cluster`` has been renamed to ``prod-cluster``.

.. code-block:: none

   sctool cluster update --prod-cluster cluster --name prod-cluster