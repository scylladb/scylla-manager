==============
Scylla Manager
==============

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

Slack channel
=============

If you have any troubles or questions regarding Scylla Manager contact us on `Scylla Manager Slack channel <https://scylladb-users.slack.com/archives/C01ERVBPWLU>`_.

Introduction
============

Scylla Manager automates database operations.
With Scylla Manager you can schedule tasks such as backups and repairs, check cluster status, and more.
Scylla Manager can manage multiple Scylla clusters and run cluster-wide tasks in a controlled and predictable way.
It is available for Scylla Enterprise customers and Scylla Open Source users.
With Scylla Open Source, Scylla Manager is limited to 5 nodes.
See the Scylla Manager Proprietary Software `License Agreement <https://www.scylladb.com/scylla-manager-software-license-agreement/>`_ for details.


Scylla Manager consists of three components:

* Server - a daemon that exposes a REST API
* sctool - a command-line interface (CLI) for interacting with the Server
* Agent - a daemon, installed on each Scylla node, the Server communicates with the Agent over HTTPS

The Server persists its data to a Scylla cluster which can run locally, or can run on an external cluster.
Optionally, but recommended, you can add Scylla Monitoring Stack to enable reporting of Scylla Manager metrics and alerts.
Scylla Manager comes with its own Scylla Monitoring Dashboard.
The diagram below presents a view on Scylla Manager managing multiple Scylla Clusters.
Scylla Manager Server has two connections with each Scylla node:

* REST API connection - used to access Scylla API and Scylla Manager Agent
* (Optional) CQL connection - used for the Scylla :ref:`Health Check <scylla-health-check>`

.. image:: images/architecture.jpg

Installation
============

To proceed with installation go to :doc:`Install Scylla Manager <install-scylla-manager>`.
