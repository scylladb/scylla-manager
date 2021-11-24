============
Health Check
============

Scylla Manager automatically adds three health check tasks when the cluster is added to the Scylla Manager and to existing clusters
during the upgrade procedure. You can see the tasks created by the healthcheck when you run
the :ref:`sctool task list <task-list>` command.

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

.. _scylla-health-check:

Scylla Health Check
-------------------

The Scylla health check task ensures that CQL native port is accessible on all the nodes.
Scylla Manager reads CQL IP address and port from the node configuration, and can automatically detect TLS/SSL connection.
There are two types of CQL health check :ref:`credentials-agnostic-health-check` and :ref:`cql-query-health-check`.

The results are available using the :ref:`sctool status <status>` command.

For example:

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

The status information is also available as a metric in Scylla Monitoring Manager dashboard.
The `healthcheck` task checks nodes every 15 seconds, the interval can be changed using :ref:`task-update` command.

The CQL column shows the CQL status, SSL indicator if SSL is enabled on a node, and time the check took.

Available statuses are:

* UP - Situation normal
* DOWN - Failed to connect to host or CQL error
* ERROR - Precondition failure, no request was sent
* UNAUTHORISED - Wrong username or password - only if ``username`` is specified for cluster
* TIMEOUT - Timeout

The REST column shows the status of Scylla Manager Server to Scylla API communication, and time the check took.

Available statuses are:

* UP - Situation normal
* DOWN - Failed to connect to host
* ERROR - Precondition failure, no request was sent
* HTTP XXX - HTTP failure and its status code
* UNAUTHORISED - Missing or Incorrect :ref:`Authentication Token <configure-auth-token>` was used
* TIMEOUT - Timeout

Error information
-----------------

.. versionadded:: 2.5 Scylla Manager

In case of error (status ERROR or DOWN) there is additional error section below the table describing the errors.

.. code-block:: none

   sctool status -c test-cluster
   Datacenter: eu-west
   ╭────┬────────────┬────────────┬──────────┬────────────────┬──────────┬──────┬──────────┬────────┬──────────┬──────────────────────────────────────╮
   │    │ Alternator │ CQL        │ REST     │ Address        │ Uptime   │ CPUs │ Memory   │ Scylla │ Agent    │ Host ID                              │
   ├────┼────────────┼────────────┼──────────┼────────────────┼──────────┼──────┼──────────┼────────┼──────────┼──────────────────────────────────────┤
   │ UN │ UP (12ms)  │ DOWN (0ms) │ UP (3ms) │ 192.168.100.11 │ 1h32m35s │ 4    │ 31.11GiB │ 4.2.1  │ 2.5.0    │ 1edbfd5b-4b1c-4bb0-afab-d69fd25db6af │
   │ UN │ UP (8ms)   │ UP (3ms)   │ UP (5ms) │ 192.168.100.12 │ 1h32m35s │ 4    │ 31.11GiB │ 4.2.1  │ 2.5.0    │ 0c0999a2-c879-4e69-9924-1641c8487bd5 │
   │ UN │ UP (10ms)  │ UP (8ms)   │ UP (1ms) │ 192.168.100.13 │ 1h32m35s │ 4    │ 31.11GiB │ 4.2.1  │ 2.5.0    │ 73e9818e-ed8d-4ea8-89e4-cf485dfd4ebe │
   ╰────┴────────────┴────────────┴──────────┴────────────────┴──────────┴──────┴──────────┴────────┴──────────┴──────────────────────────────────────╯
   Errors:
   - 192.168.100.11 CQL: dial tcp 192.168.100.11:9042: connect: connection refused

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

.. _credentials-agnostic-health-check:

Credentials agnostic health check
---------------------------------

Scylla Manager does not require database credentials to work.
CQL health check is based on sending `CQL OPTIONS frame <https://github.com/apache/cassandra/blob/trunk/doc/native_protocol_v4.spec#L302>`_ and does not start a CQL session.
This is simple and effective but does not test CQL all the way down.
For that you may consider upgrading to :ref: <cql-query-health-check>`.

.. _cql-query-health-check:

CQL query health check
----------------------

.. versionadded:: 2.2 Scylla Manager

You may specify CQL ``username`` and ``password`` flags when adding cluster to Scylla Manager using :ref:`sctool cluster add <cluster-add>` command.
It's also possible to add or change that using :ref:`sctool cluster update <cluster-update>` command.
Once Scylla Manager has CQL credential to the cluster, when performing a health check, it would try to connect to each node and execute ``SELECT now() FROM system.local`` query.


.. _scylla-alternator-health-check:

Scylla Alternator Health Check
------------------------------

.. versionadded:: 2.2 Scylla Manager

If Alternator is enabled it will check the Scylla Alternator API connectivity for all nodes in parallel. In Scylla 4.0, it uses simplified ping checking if the socket is open and if it’s responding. In Scylla 4.1+, it queries the system table.

Please check the Scylla Manager :ref:`configuration-file` to adjust timeouts for your cluster.

.. _scylla-api:

Scylla REST API Health Check
----------------------------

Checks Scylla REST API connectivity by performing single HTTP request-response cycle between Scylla Manager Server and all Scylla nodes in parallel.

Please check the Scylla Manager :ref:`configuration-file` to adjust timeouts for your cluster.
