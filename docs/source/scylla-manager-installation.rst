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

#. On the same node as you are installing Scylla Manager, download and install Scylla as a local database from the `Scylla Enterprise <https://www.scylladb.com/download/#enterprise>`_ or `Scylla Open Source <https://www.scylladb.com/download/>`_ download page. There is no need to run the Scylla setup as it is taken care of later by the ``scyllamgr_setup`` script.
#. Download and install Scylla Manager from the `Scylla Download Center <https://www.scylladb.com/download/#manager>`_.
#. Follow the entire installation procedure.

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

Before you add a cluster to Scylla Manager continue with `Setup Scylla Manager Agent <../install-agent>`_
