Status
------

The status command allows you to view information about your cluster.

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

Dynamic Status Timeouts
=======================

TIMEOUT status will be shown when a node is not responding in a timely manner to Scylla Manager probes.
Prior to Scylla Manager 2.2.0, timeouts were configurable with a fixed global value that was set in the Scylla Manager configuration file.
As Scylla Manager can manage multiple clusters, with multiple data centers across different regions, latencies will vary greatly across the clusters. 
To tackle this problem, Scylla Manager 2.2.0 introduced dynamic status timeouts.

The idea behind dynamic timeouts is to minimize manual configuration while allowing RTT variation to change dynamically according to the needs of the cluster.
This is done by calculating timeout based on a series of past measurements.

Fixed, global timeout configuration remains as an option, but if `healthcheck.dynamic_timeout.enabled` is set to "true", global configuration is ignored and dynamic timeouts are used instead.

Formula for calculating dynamic timeout is set as:

	m - mean across N most recent probes
	stddev - standard deviation across N most recent probes

	timeout = m + stdev_multiplier * max(stddev, 1ms)

Instead of setting a global fixed value for the timeout user can set the behavior of dynamic timeout by setting the following parameters:

`healthcheck.dynamic_timeout.probes` - How many past measurements to consider in calculation. This number should be decreased to not react to sudden short changes or increased otherwise.

`healthcheck.dynamic_timeout.max_timeout` - If there are no past measurements or we haven't yet fulfilled at least 10% of the required number of measurements then this value will be used as the default timeout. This should be set to value that empirically makes sense for all managed clusters.

`healthcheck.dynamic_timeout.stddev_multiplier` - Configure acceptable level of variation to the mean. Increase for stable network environments.
