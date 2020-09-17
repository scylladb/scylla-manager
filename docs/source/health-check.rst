====================
Cluster Health Check
====================

Scylla Manager automatically adds three health check tasks when the cluster is added to the Scylla Manager and to existing clusters
during the upgrade procedure. You can see the tasks created by the healthcheck when you run
the `sctool task list <../sctool/#task-list>`_ command.

For example:

.. code-block:: none

   sctool task list -c prod-cluster

returns:

.. code-block:: none

   sctool task list -c prod-cluster
   ╭─────────────────────────────────────────────────────────────┬───────────┬────────────────────────────────┬────────╮
   │ Task                                                        │ Arguments │ Next run                       │ Status │
   ├─────────────────────────────────────────────────────────────┼───────────┼────────────────────────────────┼────────┤
   │ healthcheck/8988932e-de2f-4c42-a2f8-ae3b97fd7126            │           │ 02 Apr 20 12:28:10 CEST (+15s) │ NEW    │
   | healthcheck_alternator/79170f1f-8bda-481e-8538-c3ff9894d235 │           │ 02 Apr 20 12:28:10 CEST (+15s) │ NEW    │
   │ healthcheck_rest/9b7e694d-a1e3-42f1-8ca6-d3dfd9f0d94f       │           │ 02 Apr 20 12:28:40 CEST (+1h)  │ NEW    │
   │ repair/0fd8a43b-eacf-4df8-9376-2a31b0dee6cc                 │           │ 03 Apr 20 00:00:00 CEST (+7d)  │ NEW    │
   ╰─────────────────────────────────────────────────────────────┴───────────┴────────────────────────────────┴────────╯

We can see three healthcheck related tasks:

.. include:: _common/health-check-tasks.rst

Scylla Health Check
-------------------

The Scylla health check task ensures that CQL native port is accessible on all the nodes. For each node, in parallel,
Scylla Manager opens a connection to a CQL port and asks for server options. If there is no response or the response takes longer than `configured value <../configuration-file/#health-check-settings>`_, the node is considered to be DOWN otherwise the node is considered to be UP.
The results are available using the `sctool status <../sctool/#status>` command.

For example:

.. code-block:: none

   sctool status -c prod-cluster

returns:

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

Scylla Manager reads CQL IP address and port from the node configuration, and can automatically detect TLS/SSL connection.
There are two types of CQL health check `Credentials agnostic health check`_ and `CQL query health check`_.

Node information
----------------

.. versionadded:: 2.2 Scylla Manager

Node status check also provides additional columns that show properties of the available nodes. Those are:

- CPUs - Total OS CPU count
- Memory - Total OS memory available
- Uptime - How long the system has been running without restarts
- Scylla - Version of Scylla server running on the node
- Agent - Version Scylla Manger Agent running on the node
- Host - UUID of the node
- Address - IP address of the node

Scylla Monitoring
-----------------

If you have enabled the Scylla Monitoring stack, Scylla Manager dashboard includes the same cluster status report.
In addition, the Prometheus Alert Manager has an alert to report when a Scylla node health check fails.

Credentials agnostic health check
---------------------------------

Scylla Manager does not require database credentials to work.
CQL health check is based on sending `CQL OPTIONS frame <https://github.com/apache/cassandra/blob/trunk/doc/native_protocol_v4.spec#L302>`_ and does not start a CQL session.
This is simple and effective but does not test CQL all the way down.
For that you may consider upgrading to `CQL query health check`_.

CQL query health check
----------------------

.. versionadded:: 2.2 Scylla Manager

You may specify CQL ``username`` and ``password`` flags when adding cluster to Scylla Manager using `sctool cluster add command <../sctool/#cluster-add>`_.
It's also possible to add or change that using `sctool cluster update command <../sctool/#cluster-update>`_.
Once Scylla Manager has CQL credential to the cluster, when performing a health check, it would try to connect to each node and execute ``SELECT now() FROM system.local`` query.

Scylla Alternator Health Check
------------------------------

.. versionadded:: 2.2 Scylla Manager

If Alternator is enabled it will check the Scylla Alternator API connectivity for all nodes in parallel. In Scylla 4.0 it uses simplified ping checking if the socket is open and if it’s responding. In Scylla 4.1+ it queries the system table.

Please check `configuration <../configuration-file/#health-check-settings>`_ to adjust timeouts for your cluster.

Scylla REST API Health Check
----------------------------

Checks Scylla REST API connectivity by performing single HTTP request-response cycle between Scylla Manager Server and all Scylla nodes in parallel.

Please check `configuration <../configuration-file/#health-check-settings>`_ to adjust timeouts for your cluster.