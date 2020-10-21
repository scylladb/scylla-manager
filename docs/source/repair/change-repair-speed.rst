=======================
Change the repair speed
=======================

Repair speed is controlled by two parameters: ``--intensity`` and ``--parallel``.
Those parameters can be set when you:

* Schedule a repair with :ref:`sctool repair <sctool-repair>`
* Update a repair with :ref:`sctool repair update <reschedule-a-repair>`
* Run a repair with :ref:`sctool repair control <repair-control>`

Please read the detailed information on the flags in the sctool reference: :ref:`intensity <intensity-float>`, :ref:`parallel <parallel-integer>`.

.. contents::
   :depth: 2
   :local:
.. _change-speed:

Repair faster
-------------

By default Scylla Manager runs repairs with full parallelism, the way to make repairs faster is by increasing the intensity.
In this example, try setting ``--intensity 0``, that would adjust the number of token ranges per Scylla repair job to the maximum supported (in parallel) by a repair master node.
If you want to go faster than that, you can set intensity to a higher value, but this is not recommended.
By doing so, you can make some time savings on the Scylla repair job creation and status checking.
You pay with repair granularity, and in case you need to pause or retry the repair, the amount of repeated work will be significant.

Speedup a running repair
........................

If a repair is running on a cluster, you can change the intensity and parallelism level that should be applied while it is running.
Stopping the task and running it again would reset the values.
You can view your current values for intensity and parallel with the :ref:`sctool task progress <task-progress>` command.

Run the following command to adjust the number of token ranges per Scylla repair job to the maximum supported (in parallel) by a repair master node.

.. code-block:: none

   sctool repair control -c prod-cluster --intensity 0

Speedup the future runs of a repair
...................................

If you wish to change intensity and parallelism level of a repair task that is scheduled, but not running use :ref:`sctool repair update <repair-update>`.

For example, the following command adjusts the number of token ranges per Scylla repair job to the maximum supported (in parallel) by a repair master node.

.. code-block:: none

   sctool repair update -c prod-cluster repair/143d160f-e53c-4890-a9e7-149561376cfd --intensity 0

Repair slower
-------------

You can make repair run slower by changing the level of parallelism or intensity.
By default Scylla Manager runs repairs with full parallelism.
Setting ``--parallel 1``, that caps the number of Scylla repair jobs in the cluster to 1, and gives space to some nodes.
This would have the same result as running Scylla Manager 2.1 or earlier in terms of parallelism.
For Scylla clusters that **do not support row-level repair**, you can change the number of shards that are being repaired in parallel.
In this case, setting ``--intensity 0.5``, that runs the repair on half of the shards in parallel.

.. note::
   For Scylla clusters that are row-level repair enabled, setting intensity below 1 has the **same effect** as setting intensity 1.

Slowdown a running repair
.........................

If a repair is running on a cluster, you can change intensity and parallelism levels while it is running.
Stopping the task and running again, would reset the values.
You can view your current values for intensity and parallel with the :ref:`sctool task progress <task-progress>` command.

Run the following command to limit the number of parallel Scylla repair jobs in the cluster to 1.

.. code-block:: none

   sctool repair control -c prod-cluster --parallel 1

For clusters **not supporting row-level repair**.
Run the following command to repair half of the shards on repair master node in parallel.

.. code-block:: none

   sctool repair control -c prod-cluster --intensity 0.5

Slowdown the future runs of a repair
....................................

If you wish to change intensity and parallelism level of a repair task use :ref:`sctool repair update <repair-update>`.

Run the following command to limit the number of parallel Scylla repair jobs in the cluster to 1.

.. code-block:: none

   sctool repair update -c prod-cluster repair/143d160f-e53c-4890-a9e7-149561376cfd --parallel 1

For clusters **not supporting row-level repair**.
Run the following command to repair half of the shards on repair master node in parallel.

.. code-block:: none

   sctool repair update -c prod-cluster repair/143d160f-e53c-4890-a9e7-149561376cfd --intensity 0.5
