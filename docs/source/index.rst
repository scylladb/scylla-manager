=================
ScyllaDB Manager
=================

.. toctree::
   :hidden:
   :maxdepth: 2

   docker/index
   install-scylla-manager
   install-scylla-manager-agent
   upgrade/index
   add-a-cluster
   backup/index
   restore/index
   repair/index
   health-check
   sctool/index
   config/index
   swagger/index
   scylla-monitoring
   troubleshooting
   Slack <https://scylladb-users.slack.com/archives/C01ERVBPWLU>

.. image:: images/header.png

Slack Channel
=============

If you have any troubles or questions regarding ScyllaDB Manager contact us on `ScyllaDB Manager Slack channel <https://scylladb-users.slack.com/archives/C01ERVBPWLU>`_.

Introduction
============

ScyllaDB Manager automates database operations.
With ScyllaDB Manager you can schedule tasks such as backups and repairs, check cluster status, and more.
ScyllaDB Manager can manage multiple ScyllaDB clusters and run cluster-wide tasks in a controlled and predictable way.
It is available for ScyllaDB Enterprise customers and ScyllaDB Open Source users.
With ScyllaDB Open Source, ScyllaDB Manager is limited to 5 nodes.
See the ScyllaDB Manager Proprietary Software `License Agreement <https://www.scylladb.com/scylla-manager-software-license-agreement/>`_ for details.


ScyllaDB Manager consists of three components:

* Server - a daemon that exposes a REST API
* sctool - a command-line interface (CLI) for interacting with the Server
* Agent - a daemon, installed on each ScyllaDB node, the Server communicates with the Agent over HTTPS

The Server persists its data to a ScyllaDB cluster which can run locally, or can run on an external cluster.
Optionally, but recommended, you can add ScyllaDB Monitoring Stack to enable reporting of ScyllaDB Manager metrics and alerts.
ScyllaDB Manager comes with its own ScyllaDB Monitoring Dashboard.
The diagram below presents a view on ScyllaDB Manager managing multiple ScyllaDB Clusters.
ScyllaDB Manager Server has two connections with each ScyllaDB node:

* REST API connection - used to access ScyllaDB API and ScyllaDB Manager Agent
* (Optional) CQL connection - used for the ScyllaDB :ref:`Health Check <scylla-health-check>`

.. image:: images/architecture.jpg

Installation
============

To proceed with installation go to :doc:`Install ScyllaDB Manager <install-scylla-manager>`.
