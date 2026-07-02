=============
Repair slower
=============

You can make repair run slower by changing the level of parallelism or intensity.
By default Scylla Manager runs repairs with full parallelism and intensity one.

Reduce amount of work performed by a single Scylla repair job.
If ``--intensity`` has been previously increased, try setting it to ``--intensity 1``. That reduces the number of token ranges repaired in a single Scylla repair job.
Beware that 0 is a special value, it means max intensity.

Try setting ``--parallel 1``, that would cap the number of Scylla repair jobs in the cluster to one, frees nodes that are not being repaired.
This would have the same result as running Scylla Manager 2.1 or earlier in terms of parallelism.

Slowdown a running repair
=========================

If a repair is running on a cluster, you can change intensity and parallelism levels while it is running.
Run the following command to limit the number of parallel Scylla repair jobs in the cluster to one.

.. code-block:: none

   sctool repair control -c prod-cluster --parallel 1

You can view your current values for intensity and parallel with the :ref:`sctool progress <task-progress>` command.
Stopping the task and running again, would reset the values.

Slowdown the future runs of a repair
====================================

If you wish to change intensity and parallelism level of a repair task use :ref:`sctool repair update <repair-update>` command.
Run the following command to limit the number of parallel Scylla repair jobs in the cluster to one.

.. code-block:: none

   sctool repair update -c prod-cluster repair/143d160f-e53c-4890-a9e7-149561376cfd --parallel 1
