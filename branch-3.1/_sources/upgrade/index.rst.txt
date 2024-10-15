==========================
ScyllaDB Manager Upgrade 
==========================

This document describes the upgrade process beween two versions of Scylla Manager.

Applicable versions
===================

This guide covers the general steps for upgrading Scylla Manager between any two versions on the following platforms:

- Red Hat Enterprise Linux, version 7
- CentOS, version 7
- Debian, version 9
- Ubuntu, versions 16.04, 18.04

Upgrade Procedure
=================

Upgrade procedure for the Scylla Manager includes upgrade of three components server, client, and the agent. Entire cluster shutdown is NOT needed. Scylla will be running while the manager components are upgraded. Overview of the required steps:

- Stop all Scylla Manager tasks (or wait for them to finish)
- Stop the Scylla Manager Server
- Stop the Scylla Manager Agent on all nodes
- Upgrade the Scylla Manager Server and Client
- Upgrade the Scylla Manager Agent on all nodes
- Run `scyllamgr_agent_setup` script on all nodes
- Reconcile configuration files
- Start the upgraded Scylla Manager Agent on all nodes
- Start the upgraded Scylla Manager Server
- Validate status of the cluster

Upgrade steps
=============

Stop all Scylla Manager tasks (or wait for them to finish)
----------------------------------------------------------

**On the Manager Server** check current status of the manager tasks:

.. code:: sh

    sctool task list -c <cluster>

Or on versions 3.0 and higher using:

.. code:: sh

   sctool tasks -c <cluster>

None of the listed tasks should have status in RUNNING.

Stop the ScyllaDB Manager Server 
----------------------------------

**On the Manager Server** instruct Systemd to stop the server process:

.. code:: sh

    sudo systemctl stop scylla-manager

Ensure that it is stopped with:

.. code:: sh

    sudo systemctl status scylla-manager

It should have a status of *“Active: inactive (dead)”*.

Stop the ScyllaDB Manager Agent on all nodes
------------------------------------------------

**On each scylla node** in the cluster run:

.. code:: sh

    sudo systemctl stop scylla-manager-agent

Ensure that it is stopped with:

.. code:: sh

    sudo systemctl status scylla-manager-agent

It should have a status of *“Active: inactive (dead)”*.

Upgrade the ScyllaDB Manager Server and Client 
------------------------------------------------

**Before you Begin**

Confirm that the settings for ``scylla-manager.repo`` are correct.

**On the Manager Server** display the contents of the scylla-manager.repo and confirm the version displayed is the desired version, in this example 3.0.

CentOS, Red Hat:

.. code:: sh

   cat /etc/yum.repos.d/scylla-manager.repo

   [scylla-manager-3.0] name=Scylla Manager for Centos - $basearch baseurl=http://downloads.scylladb.com/downloads/scylla-manager/rpm/centos/scylladb-manager-3.0/$basearch/ enabled=1 gpgcheck=0


Debian, Ubuntu:

.. code:: sh

   cat /etc/apt/sources.list/scylla-manager.repo

   [scylla-manager-3.0] name=Scylla Manager for Centos - $basearch baseurl=http://downloads.scylladb.com/downloads/scylla-manager/rpm/centos/scylladb-manager-3.0/$basearch/ enabled=1 gpgcheck=0


**On the Manager Server** instruct package manager to update server and the client:

CentOS, Red Hat:

.. code:: sh

    sudo yum update scylla-manager-server scylla-manager-client -y


Debian, Ubuntu:

.. code:: sh

    sudo apt-get update
    sudo apt-get install scylla-manager-server scylla-manager-client -y

.. note:: When using apt-get, if a previous version of the Scylla Manager package had a modified configuration file, you will be asked what to do with this file during the installation process. In order to keep both files for reconciliation (covered later in the procedure), select the "keep your currently-installed version" option when prompted. 

Upgrade the ScyllaDB Manager Agent on all nodes
------------------------------------------------------

**On each scylla node** instruct package manager to update the agent:

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

.. note:: Script mentioned in this section is added in version 2.0.2 so it won't be available for earlier versions.

This step requires sudo rights:

.. code:: sh

    $ sudo scyllamgr_agent_setup
    Do you want to create scylla-helper.slice if it does not exist?
    Yes - limit Scylla Manager Agent and other helper programs memory. No - skip this step.
    [YES/no] YES
    Do you want the Scylla Manager Agent service to automatically start when the node boots?
    Yes - automatically start Scylla Manager Agent when the node boots. No - skip this step.
    [YES/no] YES

First step relates to limiting resources that are available to the agent and second
instructs systemd to run agent on node restart.

Reconcile configuration files
-----------------------------

Upgrades can create changes to the structure and values of the default yaml configuration file. If the previous version's configuration file was modified with custom values, this could result in a conflict. The upgrade procedure can't resolve this without help from an administrator. If you followed instructions from the upgrade packages sections of this document, and you elected to save both the new and old configuration files, the new version of the configuration file is saved in the same directory as the old one with an added extension suffix for both server and agent. These files are stored in the `/etc/scylla-manager` directory.

On a CentOS configuration, a conflict looks like:

.. code:: sh

    # On the Scylla Manager node
    /etc/scylla-manager/scylla-manager.yaml # old file containing custom values
    /etc/scylla-manager/scylla-manager.yaml.rpmnew # new default file from new version
    # On all Scylla nodes
    /etc/scylla-manager-agent/scylla-manager-agent.yaml # old file containing custom values
    /etc/scylla-manager-agent/scylla-manager-agent.yaml.rpmnew # new default file from new version

On an Ubuntu configuration, a conflict looks like:

.. code:: sh

    # On the Scylla Manager node
    /etc/scylla-manager/scylla-manager.yaml # old file containing custom values
    /etc/scylla-manager/scylla-manager.yaml.dpkg-dist # new default file from new version
    # On all Scylla nodes
    /etc/scylla-manager-agent/scylla-manager-agent.yaml # old file containing custom values
    /etc/scylla-manager-agent/scylla-manager-agent.yaml.dpkg-dist # new default file from new version

It is required to manually inspect both files and reconcile old values with the new configuration. Remember to carry over any custom values like database credentials, backup, repair, and any other configuration. This can be done by manually updating values in the new config file and then renaming files:

For CentOS:

.. code:: sh

    # On the Scylla Manager node
    cd /etc/scylla-manager/
    mv scylla-manager.yaml scylla-manager.yaml.old  #renames the old config file as old
    mv scylla-manager.yaml.rpmnew scylla-manager.yaml
    # On all Scylla nodes
    cd /etc/scylla-manager-agent/
    mv scylla-manager-agent.yaml scylla-manager-agent.yaml.old
    mv scylla-manager-agent.yaml.rpmnew scylla-manager-agent.yaml

For Ubuntu:

.. code:: sh

    # On the Scylla Manager node
    cd /etc/scylla-manager/
    mv scylla-manager.yaml scylla-manager.yaml.old
    mv scylla-manager.yaml.dpkg-dist scylla-manager.yaml
    # On all Scylla nodes
    cd /etc/scylla-manager-agent/
    mv scylla-manager-agent.yaml scylla-manager-agent.yaml.old
    mv scylla-manager-agent.yaml.dpkg-dist scylla-manager-agent.yaml

Start the ScyllaDB Manager Agent on all nodes
-------------------------------------------------

**On each scylla node** instruct Systemd to start the agent process:

.. code:: sh

    sudo systemctl start scylla-manager-agent

Ensure that it is running with:

.. code:: sh

    sudo systemctl status scylla-manager-agent

It should have a status of *“Active: active (running)”*.

Start the ScyllaDB Manager Server 
-----------------------------------

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

Rollback Procedure
==================

.. note:: Rolling back is not recommended because updated versions contains bug fixes and performance optimizations so you will be going back to a lesser version. This should be only used as a last resort.

Rollback procedure contains the same steps as upgrade but with downgrading the components to older version:

- Stop all Scylla Manager tasks (or wait for them to finish)
- Stop the Scylla Manager Server
- Stop the Scylla Manager Agent on all nodes
- Downgrade the Scylla Manager Server and Client
- Downgrade the Scylla Manager Agent on all nodes
- Bring back old configuration (if there was conflict)
- Start the Scylla Manager Agent on all nodes
- Start the Scylla Manager Server
- Validate status of the cluster

Rollback steps
==============

Stop all ScyllaDB Manager tasks (or wait for them to finish)
--------------------------------------------------------------

**On the Manager Server** check current status of the manager tasks:

.. code:: sh

    sctool tasks -c <cluster>

None of the listed tasks should have status in RUNNING.

Stop the ScyllaDB Manager Server 
-----------------------------------

**On the Manager Server** instruct Systemd to stop the server process:

.. code:: sh

    sudo systemctl stop scylla-manager

Ensure that it is stopped with:

.. code:: sh

    sudo systemctl status scylla-manager

It should have a status of *“Active: inactive (dead)”*.

Stop the ScyllaDB Manager Agent on all nodes
------------------------------------------------

**On each scylla node** in the cluster run:

.. code:: sh

    sudo systemctl stop scylla-manager-agent

Ensure that it is stopped with:

.. code:: sh

    sudo systemctl status scylla-manager-agent

It should have a status of *“Active: inactive (dead)”*.

Downgrade the ScyllaDB Manager Server and Client 
----------------------------------------------------

**On the Manager Server** instruct package manager to downgrade server and the client:

CentOS, Red Hat:

.. code:: sh

    sudo yum downgrade scylla-manager-server-2.x* scylla-manager-client-2.x* -y

Debian, Ubuntu:

.. code:: sh

    sudo apt-get install scylla-manager-server=2.x scylla-manager-client=2.x -y

Downgrade the ScyllaDB Manager Agent on all nodes
--------------------------------------------------------

**On each scylla node** instruct package manager to downgrade the agent:

CentOS, Red Hat:

.. code:: sh

    sudo yum downgrade scylla-manager-agent-2.x* -y

Debian, Ubuntu:

.. code:: sh

    sudo apt-get install scylla-manager-agent=2.x -y

Revert to the old configuration
----------------------------------------------------

If you followed instructions from the Upgrade Steps section and you had configuration conflict when upgrading, then listing the configuration directory should give you both new and old configuration:

.. code:: sh

    /etc/scylla-manager/scylla-manager.yaml # New version that you want to disable
    /etc/scylla-manager/scylla-manager.yaml.old # Previous version that you want to rollback

To restore the old configuration:

.. code:: sh

    cd /etc/scylla-manager/
    mv scylla-manager.yaml scylla-manager.yaml.new
    mv scylla-manager.yaml.old scylla-manager.yaml

The procedure is the same for the Scylla Manager Agent (on all nodes):

.. code:: sh

    cd /etc/scylla-manager-agent/
    mv scylla-manager-agent.yaml scylla-manager-agent.yaml.new
    mv scylla-manager-agent.yaml.old scylla-manager-agent.yaml

Start the ScyllaDB Manager Agent on all nodes
-------------------------------------------------

On all nodes instruct Systemd to start the agent process:

.. code:: sh

    sudo systemctl start scylla-manager-agent

Ensure that it is running with:

.. code:: sh

    sudo systemctl status scylla-manager-agent

It should have a status of *“Active: active (running)”*.

Start the ScyllaDB Manager Server
-------------------------------------

**On the Manager Server** instruct Systemd to start the server process:

.. code:: sh

    sudo systemctl stop scylla-manager

Ensure that it is stopped with:

.. code:: sh

    sudo systemctl status scylla-manager

It should have a status of *“Active: active (running)”*.

Validate status of the cluster
------------------------------

**On the Manager Server** check the version of the client and the server:

.. code:: sh

    sctool version
    Client version: 2.x
    Server version: 2.x

Check that cluster is up:

.. code:: sh

    sctool status -c <cluster>

All running nodes should be up.
