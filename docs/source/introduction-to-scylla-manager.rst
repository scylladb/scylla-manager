==============================
Introduction to Scylla Manager
==============================

.. include:: /_common/manager-description.rst

Architecture
============

Scylla Manager consists of three components:

* Server - a daemon that exposes REST API
* sctool - a command-line interface (CLI) for interacting with the Server over the REST API
* Agent - a small executable, installed on each Scylla node. The Server communicates with the Agent over REST HTTPS. The Agent communicates with the local Scylla node over the REST HTTP.

The Server persists its data to a Scylla cluster which can run locally or can run on an external cluster
(see `Use a remote database for Scylla Manager <../use-a-remote-db>`_ for details).

Optionally (but recommended), you can add Scylla Monitoring Stack to enable reporting of Scylla Manager metrics and alerts. 

The diagram below presents a logical view of Scylla Manager with a remote backend datastore managing multiple Scylla Clusters situated in datacenters.

Each node has two connections with the Scylla Manager Server:

* REST API connection - used for Scylla Manager and Scylla Manager Agent activities
* CQL connection - used for the Scylla `Health Check <../health-check>`_

.. image:: images/architecture.jpg

Additional Resources
====================

* `Install Scylla Manager <../install>`_
* `Install Scylla Manager Agent <../install-agent>`_
* `sctool Reference <../sctool>`_
