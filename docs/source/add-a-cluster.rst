=============
Add a Cluster
=============

**Prerequisites**

* Scylla Manager Agent is up and running on all Scylla nodes.
* All the Agents have the same :ref:`authentication token <configure-auth-token>` configured.
* Traffic on the following ports is unblocked from the Scylla Manager Server to all the Scylla nodes.

  * ``10001`` - Scylla Manager Agent REST API (HTTPS)
  * CQL port (typically ``9042``) - required for CQL health check status reports

.. _add-cluster:

**Procedure**

#. From the Scylla Manager Server, provide the IP address of one of the nodes, the generated auth token, and a custom name.

   Example (IPv4):

   .. code-block:: none

      sctool cluster add --host 34.203.122.52 --name prod-cluster \
      --auth-token "6Es3dm24U72NzAu9ANWmU3C4ALyVZhwwPZZPWtK10eYGHJ24wMoh9SQxRZEluWMc0qDrsWCCshvfhk9uewOimQS2x5yNTYUEoIkO1VpSmTFu5fsFyoDgEkmNrCJpXtfM"

      c1bbabf3-cad1-4a59-ab8f-84e2a73b623f
       __  
      /  \     Cluster added! You can set it as default, by exporting env variable.
      @  @     $ export SCYLLA_MANAGER_CLUSTER=c1bbabf3-cad1-4a59-ab8f-84e2a73b623f
      |  |     $ export SCYLLA_MANAGER_CLUSTER=prod-cluster
      || |/    
      || ||    Now run:
      |\_/|    $ sctool status -c prod-cluster
      \___/    $ sctool tasks -c prod-cluster


   Example (IPv6):

   .. code-block:: none

      sctool cluster add --host 2a05:d018:223:f00:971d:14af:6418:fe2d --name prod-cluster \
      --auth-token "6Es3dm24U72NzAu9ANWmU3C4ALyVZhwwPZZPWtK10eYGHJ24wMoh9SQxRZEluWMc0qDrsWCCshvfhk9uewOimQS2x5yNTYUEoIkO1VpSmTFu5fsFyoDgEkmNrCJpXtfM"

   Where:

   * ``--host`` is hostname or IP of one of the cluster nodes. You can use an IPv6 or an IPv4 address.
   * ``--name`` is an alias you can give to your cluster.
     Using an alias means you do not need to use the ID of the cluster in all other operations.
     This name must be used when connecting the managed cluster to Scylla Monitor, but does not have to be the same name you used in scylla.yaml.
   * ``--auth-token`` is the :ref:`authentication token <configure-auth-token>` you generated.

   Each cluster has a unique ID (UUID), you will see it printed to stdout in ``sctool cluster add`` output when the cluster is added.
   You will use this ID in all commands where the cluster ID is required.
   Alternatively you can use the name you assigned to the cluster instead of the ID.
   You can also set a custom UUID using ``--id <my-UUID>`` flag.

   Each cluster is automatically registered with a repair task which runs once a week.
   This can be canceled using ``--without-repair``.
   To use a different repair schedule, see :ref:`Schedule a Repair <schedule-a-repair>`.

   Optionally you can provide CQL credentials to the cluster with ``--username`` and ``--password`` flags.
   This enables :ref:`CQL query based health check <cql-query-health-check>` compared to :ref:`credentials agnostic health check <credentials-agnostic-health-check>` if you do not specify the credentials.
   This also enables CQL schema backup in text format, which isn't performed if credentials aren't provided.
   For security reasons the CQL user should NOT have access to read your data.

#. Verify the cluster you added has a registered repair task by running the ``sctool tasks`` command.

   .. code-block:: none

      sctool tasks
      Cluster: prod-cluster (c1bbabf3-cad1-4a59-ab8f-84e2a73b623f)
      ╭────────────────────────┬──────────────┬────────┬──────────────────┬─────────┬───────┬──────────────┬────────────┬─────────┬────────────────╮
      │ Task                   │ Schedule     │ Window │ Timezone         │ Success │ Error │ Last Success │ Last Error │ Status  │ Next           │
      ├────────────────────────┼──────────────┼────────┼──────────────────┼─────────┼───────┼──────────────┼────────────┼─────────┼────────────────┤
      │ healthcheck/cql        │ @every 15s   │        │ America/New_York │ 4       │ 0     │ 1s ago       │            │ DONE    │ in 13s         │
      │ healthcheck/alternator │ @every 15s   │        │ America/New_York │ 3       │ 0     │ 14s ago      │            │ RUNNING │                │
      │ healthcheck/rest       │ @every 1m0s  │        │ America/New_York │ 1       │ 0     │ 1s ago       │            │ DONE    │ in 58s         │
      │ repair/all-weekly      │ 0 23 * * SAT │        │ America/New_York │ 0       │ 0     │              │            │ NEW     │ in 2d13h30m55s │
      ╰────────────────────────┴──────────────┴────────┴──────────────────┴─────────┴───────┴──────────────┴────────────┴─────────┴────────────────╯

   You will see 4 tasks which are created by adding the cluster:

   .. include:: common/health-check-tasks.rst

   * Repair - an automated repair task, starting at midnight tonight, repeating every seven days at midnight.

   .. note:: If you want to change the schedule for the repair, use the :ref:`repair update sctool <reschedule-a-repair>` command.

#. Verify Scylla Manager can communicate with all the Agents, and the the cluster status is OK by running the ``sctool status`` command.

   .. code-block:: none

      sctool status
      Cluster: prod-cluster (c1bbabf3-cad1-4a59-ab8f-84e2a73b623f)
      ╭────┬────────────┬───────────┬───────────┬───────────────┬────────┬──────┬──────────┬────────┬──────────┬──────────────────────────────────────╮
      │    │ Alternator │ CQL       │ REST      │ Address       │ Uptime │ CPUs │ Memory   │ Scylla │ Agent    │ Host ID                              │
      ├────┼────────────┼───────────┼───────────┼───────────────┼────────┼──────┼──────────┼────────┼──────────┼──────────────────────────────────────┤
      │ UN │ UP (4ms)   │ UP (3ms)  │ UP (2ms)  │ 34.203.122.52 │ 2m1s   │ 4    │ 15.43GiB │ 4.1.0  │ 3.0.0    │ 8bfd18f1-ac3b-4694-bcba-30bc272554df │
      │ UN │ UP (15ms)  │ UP (11ms) │ UP (12ms) │ 10.0.138.46   │ 2m1s   │ 4    │ 15.43GiB │ 4.1.0  │ 3.0.0    │ 238acd01-813c-4c55-bd65-5219bb19bc20 │
      │ UN │ UP (17ms)  │ UP (5ms)  │ UP (7ms)  │ 10.0.196.204  │ 2m1s   │ 4    │ 15.43GiB │ 4.1.0  │ 3.0.0    │ bde4581a-b25e-49fc-8cd9-1651d7683f80 │
      │ UN │ UP (10ms)  │ UP (4ms)  │ UP (5ms)  │ 10.0.66.115   │ 2m1s   │ 4    │ 15.43GiB │ 4.1.0  │ 3.0.0    │ 918a52aa-cc42-43a4-a499-f7b1ccb53b18 │
      ╰────┴────────────┴───────────┴───────────┴───────────────┴────────┴──────┴──────────┴────────┴──────────┴──────────────────────────────────────╯
