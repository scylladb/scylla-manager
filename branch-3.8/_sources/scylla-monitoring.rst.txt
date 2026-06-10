====================
ScyllaDB Monitoring
====================

ScyllaDB Manager as Service Discovery for ScyllaDB Monitoring
===============================================================

Scylla Manager provides Consul's Service Catalog API that is used by Prometheus to dynamically get scrape targets.
ScyllaDB Monitoring uses this mechanism to discover all clusters and nodes under ScyllaDB Manager.
More information can be found in `ScyllaDB Monitoring docs <https://monitoring.docs.scylladb.com>`_.

ScyllaDB Manager Dashboard
============================

ScyllaDB Manager has its own dashboard in ScyllaDB Monitoring.
It can be used to track progress of ScyllaDB Manager tasks.

..
   TODO Add screenshot
