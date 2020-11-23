=============
Repair slower
=============

You can make repair run slower by changing the level of parallelism or intensity.
By default Scylla Manager runs repairs with full parallelism and intensity one.

Try setting ``--parallel 1``, that would cap the number of Scylla repair jobs in the cluster to one, and gives space to some nodes.
This would have the same result as running Scylla Manager 2.1 or earlier in terms of parallelism.

For Scylla clusters that **do not support row-level repair**, you can change the number of shards that are being repaired in parallel.
In this case, setting ``--intensity 0.5``, that runs the repair on half of the shards in parallel.

.. note::
   For Scylla clusters that are row-level repair enabled, setting intensity below 1 has the **same effect** as setting intensity 1.

Slowdown a running repair
=========================

If a repair is running on a cluster, you can change intensity and parallelism levels while it is running.
Run the following command to limit the number of parallel Scylla repair jobs in the cluster to one.

.. code-block:: none

   sctool repair control -c prod-cluster --parallel 1

You can view your current values for intensity and parallel with the :ref:`sctool task progress <task-progress>` command.
Stopping the task and running again, would reset the values.

**Scylla 2019 and earlier**

Run the following command to repair half of the shards on repair master node in parallel.

.. code-block:: none

   sctool repair control -c prod-cluster --intensity 0.5

Slowdown the future runs of a repair
====================================

If you wish to change intensity and parallelism level of a repair task use :ref:`sctool repair update <repair-update>` command.
Run the following command to limit the number of parallel Scylla repair jobs in the cluster to one.

.. code-block:: none

   sctool repair update -c prod-cluster repair/143d160f-e53c-4890-a9e7-149561376cfd --parallel 1

**Scylla 2019 and earlier**

Run the following command to repair half of the shards on repair master node in parallel.

.. code-block:: none

   sctool repair update -c prod-cluster repair/143d160f-e53c-4890-a9e7-149561376cfd --intensity 0.5
