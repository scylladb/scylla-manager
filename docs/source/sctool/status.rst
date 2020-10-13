Status
------

The stauts command allows you to view information about your cluster.

.. _status:

status
======

The status command is an extended version of ``nodetool status``.
It can show status for all the managed clusters.
The first column shows node status in `nodetool status <https://docs.scylladb.com/operating-scylla/nodetool-commands/status>`_ format.
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
* UNAUTHORISED - Missing or Incorrect :ref:`Authentication Token <configure-auth-token>` was used
* TIMEOUT - Timeout

The status information is also available as a metric in Scylla Monitoring Manager dashboard.
The `healthcheck` task checks nodes every 15 seconds, the interval can be changed using :ref:`task-update` command.

**Syntax:**

.. code-block:: none

   sctool status [global flags]

status parameters
..................

status takes the :ref:`global-flags`.

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
