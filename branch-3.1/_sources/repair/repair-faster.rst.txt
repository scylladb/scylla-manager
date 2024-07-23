=============
Repair faster
=============

By default Scylla Manager runs repairs with full parallelism, the way to make repairs faster is by increasing intensity.
Note that the less the cluster is loaded the more it makes sense to increase intensity.
If you increase intensity on a loaded cluster it may not give speed benefits since cluster have no resources to process more repairs.
In our experiments in a 50% loaded cluster increasing intensity from 1 to 2 gives about 10-20% boost and increasing it further have little impact.
If the cluster is idle try setting ``--intensity 0``.
If the cluster is running under substantial load try setting ``--intensity 2`` and then increase by one if needed.

Speedup a running repair
========================

If a repair is running on a cluster, you can change the intensity and parallelism level that should be applied while it is running.
Run the following command to adjust the number of token ranges per Scylla repair job to the maximum supported (in parallel) by a repair master node.

.. code-block:: none

   sctool repair control -c prod-cluster --intensity 0

You can view your current values for intensity and parallel with the :ref:`sctool progress <task-progress>` command.
Stopping the task and running it again would reset the values.

Speedup the future runs of a repair
===================================

If you wish to change intensity or parallelism of a repair task that is scheduled, but not running use :ref:`sctool repair update <repair-update>` command.
For example, the following command adjusts the number of token ranges per Scylla repair job to the maximum supported (in parallel) by a repair master node.

.. code-block:: none

   sctool repair update -c prod-cluster repair/143d160f-e53c-4890-a9e7-149561376cfd --intensity 0
