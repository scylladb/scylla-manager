=========================
ScyllaDB Manager Upgrade 
=========================

This document describes the upgrade process between two versions of ScyllaDB Manager.

.. note:: See :doc:`../compatibility-matrix` for information about compatibility between ScyllaDB Manager and ScyllaDB versions.

Applicable platforms
====================

This guide covers the general steps for upgrading ScyllaDB Manager between any two versions on the following platforms:

- Red Hat Enterprise Linux, versions 8 and 9
- Debian, versions 11 and 12
- Ubuntu, versions 22.04 and 24.04

Upgrade Procedure
=================

Upgrade procedure for the Scylla Manager includes upgrade of three components server, client, and the agent. Entire cluster shutdown is NOT needed. ScyllaDB will be running while the manager components are upgraded. Overview of the required steps:

- Stop all ScyllaDB Manager tasks (or wait for them to finish)
- Stop the ScyllaDB Manager Server
- Stop the ScyllaDB Manager Agent on all nodes
- Upgrade the ScyllaDB Manager Server and Client
- Upgrade the ScyllaDB Manager Agent on all nodes
- Run `scyllamgr_agent_setup` script on all nodes
- Reconcile configuration files
- Start the upgraded ScyllaDB Manager Agent on all nodes
- Start the upgraded ScyllaDB Manager Server
- Validate status of the cluster

Upgrade steps
=============

Stop all ScyllaDB Manager tasks (or wait for them to finish)
------------------------------------------------------------

**On the Manager Server** check current status of the manager tasks:

.. code:: sh

   sctool tasks -c <cluster>

None of the listed tasks should have status in RUNNING.

Stop the ScyllaDB Manager Server 
--------------------------------

**On the Manager Server** instruct Systemd to stop the server process:

.. code:: sh

    sudo systemctl stop scylla-manager

Ensure that it is stopped with:

.. code:: sh

    sudo systemctl status scylla-manager

It should have a status of *“Active: inactive (dead)”*.

Stop the ScyllaDB Manager Agent on all nodes
--------------------------------------------

**On each ScyllaDB node** in the cluster run:

.. code:: sh

    sudo systemctl stop scylla-manager-agent

Ensure that it is stopped with:

.. code:: sh

    sudo systemctl status scylla-manager-agent

It should have a status of *“Active: inactive (dead)”*.

Upgrade the ScyllaDB Manager Server and Client 
----------------------------------------------

.. TODO This section must be updated when the installation instructions are moved to the docs:
.. The link should take the user to the relevant page in the docs, not to the Download Center.

#. **On the Manager server**, update the Manager repo file. Go to ScyllaDB Manager in 
   the `ScyllaDB Download Center <https://www.scylladb.com/download/#manager>`_, 
   and select your platform and the version to which you want to upgrade to display the relevant command.
   
   The following examples show how to update the repo for Manager 3.6:

   .. tabs::

      .. group-tab:: Example for Centos

           .. code:: console
              :class: hide-copy-button

              sudo curl -o /etc/yum.repos.d/scylla-manager.repo -L https://downloads.scylladb.com/rpm/centos/scylladb-manager-3.6.repo

      .. group-tab::  Example for Ubuntu

           .. code:: console
              :class: hide-copy-button

              sudo wget -O /etc/apt/sources.list.d/scylla-manager.list https://downloads.scylladb.com/deb/ubuntu/scylladb-manager-3.6.list

   .. note:: 
    
     You don't need to update the repo file if you upgrade to a patch release, for example, 
     from Manager 3.5.0 to 3.5.1.

   You can display the contents of the Manager repo file to confirm that the displayed version 
   is the ver#4487sion to which you want to upgrade.

     - On CentOS/Red Hat, run: ``cat /etc/yum.repos.d/scylla-manager.repo``
     - On Debian/Ubuntu, run: ``cat /etc/apt/sources.list.d/scylla-manager.list``

#. **On the Manager server**, instruct the package manager to update the server and the client:

   .. tabs::

      .. group-tab:: CentOS/ Red Hat

        .. code:: console
            
            sudo yum update scylla-manager-server scylla-manager-client -y

      .. group-tab:: Debian/ Ubuntu

        .. code:: console
            
            sudo apt-get update
            sudo apt-get install scylla-manager-server scylla-manager-client -y

        .. note:: When using apt-get, if a previous version of the ScyllaDB Manager package had a modified configuration file, you will be asked what to do with this file during the installation process. In order to keep both files for reconciliation (covered later in the procedure), select the "keep your currently-installed version" option when prompted.


Upgrade the ScyllaDB Manager Agent on all nodes
-----------------------------------------------

**On each ScyllaDB node** instruct package manager to update the agent:

CentOS, Red Hat:

.. code:: sh

    sudo yum update scylla-manager-agent -y

Debian, Ubuntu:

.. code:: sh

    sudo apt-get update
    sudo apt-get install scylla-manager-agent -y

.. note:: With apt-get, if a previous version of the package had a modified configuration file, you will be asked during installation what to do with it. Please select "keep your currently-installed version" option to keep both previous and new default configuration file for later reconciliation.

Run `scyllamgr_agent_setup` script on all nodes
-----------------------------------------------

This step requires sudo rights:

.. code:: sh

    $ sudo scyllamgr_agent_setup
    Do you want to create scylla-helper.slice if it does not exist?
    Yes - limit ScyllaDB Manager Agent and other helper programs memory. No - skip this step.
    [YES/no] YES
    Do you want the ScyllaDB Manager Agent service to automatically start when the node boots?
    Yes - automatically start ScyllaDB Manager Agent when the node boots. No - skip this step.
    [YES/no] YES

First step relates to limiting resources that are available to the agent and second
instructs systemd to run agent on node restart.

Reconcile configuration files
-----------------------------

Upgrades can create changes to the structure and values of the default yaml configuration file. If the previous version's configuration file was modified with custom values, this could result in a conflict. The upgrade procedure can't resolve this without help from an administrator. If you followed instructions from the upgrade packages sections of this document, and you elected to save both the new and old configuration files, the new version of the configuration file is saved in the same directory as the old one with an added extension suffix for both server and agent. These files are stored in the `/etc/scylla-manager` directory.

On a CentOS configuration, a conflict looks like:

.. code:: sh

    # On the ScyllaDB Manager node
    /etc/scylla-manager/scylla-manager.yaml # old file containing custom values
    /etc/scylla-manager/scylla-manager.yaml.rpmnew # new default file from new version
    # On all Scylla nodes
    /etc/scylla-manager-agent/scylla-manager-agent.yaml # old file containing custom values
    /etc/scylla-manager-agent/scylla-manager-agent.yaml.rpmnew # new default file from new version

On an Ubuntu configuration, a conflict looks like:

.. code:: sh

    # On the ScyllaDB Manager node
    /etc/scylla-manager/scylla-manager.yaml # old file containing custom values
    /etc/scylla-manager/scylla-manager.yaml.dpkg-dist # new default file from new version
    # On all ScyllaDB nodes
    /etc/scylla-manager-agent/scylla-manager-agent.yaml # old file containing custom values
    /etc/scylla-manager-agent/scylla-manager-agent.yaml.dpkg-dist # new default file from new version

It is required to manually inspect both files and reconcile old values with the new configuration. Remember to carry over any custom values like database credentials, backup, repair, and any other configuration. This can be done by manually updating values in the new config file and then renaming files:

For CentOS:

.. code:: sh

    # On the Scylla Manager node
    cd /etc/scylla-manager/
    mv scylla-manager.yaml scylla-manager.yaml.old  #renames the old config file as old
    mv scylla-manager.yaml.rpmnew scylla-manager.yaml
    # On all ScyllaDB nodes
    cd /etc/scylla-manager-agent/
    mv scylla-manager-agent.yaml scylla-manager-agent.yaml.old
    mv scylla-manager-agent.yaml.rpmnew scylla-manager-agent.yaml

For Ubuntu:

.. code:: sh

    # On the ScyllaDB Manager node
    cd /etc/scylla-manager/
    mv scylla-manager.yaml scylla-manager.yaml.old
    mv scylla-manager.yaml.dpkg-dist scylla-manager.yaml
    # On all ScyllaDB nodes
    cd /etc/scylla-manager-agent/
    mv scylla-manager-agent.yaml scylla-manager-agent.yaml.old
    mv scylla-manager-agent.yaml.dpkg-dist scylla-manager-agent.yaml

.. note:: If you plan to use native backup after the upgrade, we recommend :ref:`configuring <configure-native-backup-in-scylla>` it at this point.

Start the ScyllaDB Manager Agent on all nodes
---------------------------------------------

**On each ScyllaDB node** instruct Systemd to start the agent process:

.. code:: sh

    sudo systemctl start scylla-manager-agent

Ensure that it is running with:

.. code:: sh

    sudo systemctl status scylla-manager-agent

It should have a status of *“Active: active (running)”*.

Start the ScyllaDB Manager Server 
---------------------------------

**On the Manager Server** instruct Systemd to start the server process:

.. code:: sh

    sudo systemctl daemon-reload
    sudo systemctl start scylla-manager

Ensure that it is started with:

.. code:: sh

    sudo systemctl status scylla-manager

It should have a status of *“Active: active (running)”*.

Validate status of the cluster
------------------------------

**On the Manager Server** check the version of the client and the server:

.. code:: sh

    sctool version
    Client version: 3.x.y-0.20200123.7cf18f6b
    Server version: 3.x.y-0.20200123.7cf18f6b

Check that cluster is up:

.. code:: sh

    sctool status -c <cluster>

All running nodes should be up.
