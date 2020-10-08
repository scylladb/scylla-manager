===========================
Scylla Manager Installation
===========================

.. contents::
   :depth: 2
   :local:

System requirements
===================

While a minimal server can run on a system with 2 cores and 1GB RAM, the following configuration is recommended:

* **CPU** - 2vCPUs
* **Memory** - 8GB+ DRAM

.. note::  If you are running `Scylla Monitoring Stack </operating-scylla/monitoring/monitoring_stack/>`_ on the same server as Scylla Manager, your system should also meet the minimal `Monitoring requirements </operating-scylla/monitoring/monitoring_stack/#minimal-production-system-recommendations>`_.

Install package
===============

Best practice is to install Scylla Manager Server on a dedicated machine not a Scylla production node.
Download and install the Scylla Manager Server and Client packages from the `Scylla Download Center <https://www.scylladb.com/download/#manager>`_.

Configure storage
=================

Scylla Manager uses Scylla to store its data.
You can either use a local one-node Scylla cluster (recommended) or connect Scylla Manager to a remote cluster.

Local node
----------

On the same node as you are installing Scylla Manager, download and install Scylla Server package.
You can either use `Scylla Enterprise <https://www.scylladb.com/download/#enterprise>`_ or `Scylla Open Source <https://www.scylladb.com/download/#open-source>`_.
There is no need to run the Scylla setup, it is taken care of later, by the ``scyllamgr_setup`` script.
When it's installed you can jump to `Run the scyllamgr_setup script`_ section.

Remote cluster
--------------

Scylla Manager configuration file ``/etc/scylla-manager/scylla-manager.yaml`` contains a database configuration section.

.. code-block:: yaml

   # Scylla Manager database, used to store management data.
   database:
     hosts:
       - 127.0.0.1
   # Enable or disable client/server encryption.
   #  ssl: false
   #
   # Database credentials.
   #  user: user
   #  password: password
   #
   # Local datacenter name, specify if using a remote, multi-dc cluster.
   #  local_dc:
   #
   # Database connection timeout.
   #  timeout: 600ms
   #
   # Keyspace for management data, for create statement see /etc/scylla-manager/create_keyspace.cql.tpl.
   #  keyspace: scylla_manager
   #  replication_factor: 1

Using an editor open the file and change relevant parameters.

**Procedure**

#. Edit the ``hosts`` parameter, change the IP address to the IP address or addressees of the remote cluster.

#. If authentication is needed, uncomment and edit the ``user`` and ``password`` parameters.


#. If it's a single DC cluster, uncomment and edit the ``replication_factor`` parameter to match the required replication factor.
   This would use SimpleStrategy to create a Scylla Manager keyspace, refer to `Scylla Architecture - Fault Tolerance </architecture/architecture-fault-tolerance>`_ for more information on replication.

#. If it's a multi DC cluster, create a keyspace named ``scylla_manager`` yourself.
   You can use a different keyspace name, just remember to adjust the ``keyspace`` parameter value.
   Set ``local_dc`` parameter to DC the closest to Scylla Manager Server.

#. If client/server encryption is enabled, uncomment and set the ``ssl`` parameter to ``true``.
   Additional SSL configuration options can be set in the ``ssl`` configuration section.

.. code-block:: yaml

   # Optional custom client/server encryption options.
   #ssl:
   # CA certificate used to validate server cert. If not set will use he host's root CA set.
   #  cert_file:
   #
   # Verify the hostname and server cert.
   #  validate: true
   #
   # Client certificate and key in PEM format. It has to be provided when
   # client_encryption_options.require_client_auth=true is set on server.
   #  user_cert_file:
   #  user_key_file

Sample configuration of Scylla Manager working with a remote cluster with authentication and replication factor 3 could look like this.

.. code-block:: yaml

   database:
     hosts:
       - 198.100.51.11
       - 198.100.51.12
     user: user
     password: password
     replication_factor: 3

Run the scyllamgr_setup script
==============================

The Scylla Manager setup script automates configuration of Scylla Manager by asking you some questions.
It can be run in non-interactive mode by using flags.

.. code-block:: none

   scyllamgr_setup -h
   Usage: scyllamgr_setup [-y][--no-scylla-setup][--no-enable-service][--no-check-for-updates]

   Options:
     -y, --assume-yes          assume that the answer to any question which would be asked is yes
     --no-scylla-setup         skip setting up and enabling local Scylla instance as a storage backend for Scylla Manager
     --no-enable-service       skip enabling service
     --no-check-for-updates    skip enabling periodic check for updates
     -h, --help                print this help

   Interactive mode is enabled when no flags are provided.

Run the ``scyllamgr_setup`` script to configure the service.

Enable bash script completion
=============================

Enable bash completion for sctool (the Scylla Manager CLI) in the current bash session.
Alternatively, you can just open a new terminal.

.. code-block:: none

   source /etc/bash_completion.d/sctool.bash

Start Scylla Manager service
============================

Scylla Manager integrates with ``systemd`` and can be started and stopped using ``systemctl`` command. 

**Procedure**

#. Start the Scylla Manager server service.

   .. code-block:: none

      sudo systemctl start scylla-manager.service

#. Verify the Scylla Manager server service is running.

   .. code-block:: none
      
      sudo systemctl status scylla-manager.service -l
      ● scylla-manager.service - Scylla Manager Server
         Loaded: loaded (/usr/lib/systemd/system/scylla-manager.service; enabled; vendor preset: disabled)
         Active: active (running) since Wed 2019-10-30 11:00:01 UTC; 20s ago
       Main PID: 5805 (scylla-manager)
         CGroup: /system.slice/scylla-manager.service
                 └─5805 /usr/bin/scylla-manager

   .. note:: The first time you start Scylla Manager it may take a while. It creates a database schema. Follow the logs to see the progress.

#. Confirm sctool is running by displaying the sctool version.

   .. code-block:: none

      sctool version
      Client version: 2.1-0.20200401.ce91f2ad
      Server version: 2.1-0.20200401.ce91f2ad


Next steps
==========

* `Scylla Manager Agent Installation <../scylla-manager-agent-installation>`_
