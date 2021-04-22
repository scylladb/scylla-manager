=============
Repair slower
=============

You can make repair run slower by changing the level of parallelism or intensity.
By default Scylla Manager runs repairs with full parallelism and intensity one.

Reduce amount of work performed by repair master node.
Try setting ``--intensity 0.5``, that reduces the number of token ranges repaired by a repair master node in a single chunk of work.
You can further decrease it to almost 0 forcing Scylla to repair a single token range at a time.
Beware that 0 is a special value, it means max intensity.

Try setting ``--parallel 1``, that would cap the number of Scylla repair jobs in the cluster to one, frees nodes that are not being repaired.
This would have the same result as running Scylla Manager 2.1 or earlier in terms of parallelism.

Slowdown a running repair
=========================

If a repair is running on a cluster, you can change intensity and parallelism levels while it is running.
Run the following command to repair less token ranges on nodes at the same time.
In the following example the number of token ranges equals half of the number of shards of a node.

.. code-block:: none

   sctool repair control -c prod-cluster --intensity 0.5

Run the following command to limit the number of parallel Scylla repair jobs in the cluster to one.

.. code-block:: none

   sctool repair control -c prod-cluster --parallel 1

You can view your current values for intensity and parallel with the :ref:`sctool task progress <task-progress>` command.
Stopping the task and running again, would reset the values.

Slowdown the future runs of a repair
====================================

If you wish to change intensity and parallelism level of a repair task use :ref:`sctool repair update <repair-update>` command.
Run the following command to repair less token ranges on nodes at the same time.
In the following example the number of token ranges equals half of the number of shards of a node.

.. code-block:: none

   sctool repair update -c prod-cluster repair/143d160f-e53c-4890-a9e7-149561376cfd --intensity 0.5

Run the following command to limit the number of parallel Scylla repair jobs in the cluster to one.

.. code-block:: none

   sctool repair update -c prod-cluster repair/143d160f-e53c-4890-a9e7-149561376cfd --parallel 1
