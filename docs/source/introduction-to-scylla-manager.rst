==============================
Introduction to Scylla Manager
==============================

.. include:: /_common/manager-description.rst

Architecture
============

Scylla Manager consists of three components:

* Server - a daemon that exposes a REST API
* sctool - a command-line interface (CLI) for interacting with the Server
* Agent - a daemon, installed on each Scylla node, the Server communicates with the Agent over HTTPS

The Server persists its data to a Scylla cluster which can run locally, or can run on an external cluster.
Optionally, but recommended, you can add Scylla Monitoring Stack to enable reporting of Scylla Manager metrics and alerts.
Scylla Manager comes with its own Scylla Monitoring Dashboard.
The diagram below presents a view on Scylla Manager with a remote backend datastore, managing multiple Scylla Clusters.
Each node has two connections with the Scylla Manager Server:

* REST API connection - used for Scylla Manager and Scylla Manager Agent activities
* CQL connection - used for the Scylla `Health Check <../health-check>`_

.. image:: images/architecture.jpg

Additional Resources
====================

* `Install Scylla Manager <../install>`_
* `Install Scylla Manager Agent <../install-agent>`_
* `sctool Reference <../sctool>`_
