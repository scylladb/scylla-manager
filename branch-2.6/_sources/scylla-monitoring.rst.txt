=================
Scylla Monitoring
=================

Scylla Manager as Service Discovery for Scylla Monitoring
=========================================================

Scylla Manager provides Consul's Service Catalog API that is used by Prometheus to dynamically get scrape targets.
Scylla Monitoring uses this mechanism to discover all clusters and nodes under Scylla Manager.
More information can be found in `Scylla Monitoring docs <https://scylladb.github.io/scylla-monitoring>`_.

Scylla Manager Dashboard
========================

Scylla Manager has it's own dashboard in Scylla Monitoring.
It can be used to track progress of Scylla Manager tasks.

..
   TODO Add screenshot
