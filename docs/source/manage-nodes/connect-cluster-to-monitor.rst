Connect Managed Cluster to Scylla Monitoring
============================================

Connecting your cluster to Scylla Monitoring allows you to see metrics about your cluster and Scylla Manager all within Scylla Monitoring.

To connect your Managed cluster to Scylla Monitoring it is **required** to use the same Scylla Manager cluster name as you used when you created the managed cluster. See :ref:`add-cluster`.

**Procedure**

Follow the `Scylla Monitoring <http://scylladb.github.io/scylla-monitoring/master/monitoring_stack.html#install-scylla-monitoring>` procedure as directed, remembering to update the Scylla Node IPs and Cluster name (See :ref:`add-cluster`) as well as the Scylla Manager IP in the relevant Prometheus configuration files.

If you have any issues connecting to Scylla Monitoring Stack consult the `Troubleshooting Guide <https://docs.scylladb.com/troubleshooting/manager_monitoring_integration/>`_.

* `Remove a node from a Scylla Cluster <https://docs.scylladb.com/operating-scylla/procedures/cluster-management/remove_node>`_
* `Scylla Monitoring <Scylla Monitoring <http://scylladb.github.io/scylla-monitoring>`_

