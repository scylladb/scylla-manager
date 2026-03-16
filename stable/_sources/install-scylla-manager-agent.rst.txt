.. _install-agent:

==============================
Install ScyllaDB Manager Agent
==============================

ScyllaDB Manager Agent is a daemon, that needs to be **installed and started on each ScyllaDB node**.
ScyllaDB Manager Server communicates with nodes in the managed ScyllaDB clusters via the Agents.
The communication is encrypted (HTTPS) and protected by an auth token.
Agent serves as a reverse proxy to ScyllaDB REST API, and provides additional features specific to ScyllaDB Manager.

.. note:: Repeat the procedure for **every** ScyllaDB node in the cluster that you want to be managed by ScyllaDB Manager.

.. contents::
   :depth: 2
   :local:

Install package
===============

.. tabs::

   .. group-tab:: Debian/Ubuntu

      #. Import the ScyllaDB GPG signing key. *Note*: If ScyllaDB is already installed on this node, the key was
         imported during ScyllaDB installation.

            .. code-block:: console

               sudo mkdir -p /etc/apt/keyrings
               sudo gpg --homedir /tmp --no-default-keyring --keyring /etc/apt/keyrings/scylladb.gpg --keyserver hkp://keyserver.ubuntu.com:80 --recv-keys a43e06657bac99e3

      #. Add the ScyllaDB Manager APT repository to your system.

            .. code-block:: console
               :substitutions:

               sudo curl -o /etc/apt/sources.list.d/scylla-manager.list -L https://downloads.scylladb.com/deb/debian/|UBUNTU_SCYLLADB_LIST|

      #. Install ScyllaDB Manager Agent.

            .. code-block:: console

               sudo apt update
               sudo apt install -y scylla-manager-agent

   .. group-tab:: Centos/RHEL

        #. Add the ScyllaDB Manager RPM repository to your system.

            .. code-block:: console
               :substitutions:

               sudo curl -o /etc/yum.repos.d/scylla-manager.repo -L https://downloads.scylladb.com/rpm/centos/|CENTOS_SCYLLADB_REPO|

      #. Install ScyllaDB Manager Agent. 

            .. code-block:: console

               sudo yum install scylla-manager-agent

Run the scyllamgr_agent_setup script
====================================

The ScyllaDB Manager Agent setup script automates configuration of ScyllaDB Manager Agent by asking you some questions.
It can be run in non-interactive mode by using flags.
You will need to run this command as root or with sudo.

.. note:: Make sure you run the ScyllaDB Manager Agent setup script, and enable ScyllaDB helper slice.
   The helper slice contains a cgroup definition that governs ScyllaDB Manager Agent resources usage.
   Without the slice the node latency during backup upload maybe unpredictable.

.. code-block:: none

   scyllamgr_agent_setup -h
   Usage: scyllamgr_agent_setup [-y][--no-scylla-helper-slice]

   Options:
     -y, --assume-yes          assume that the answer to any question which would be asked is yes
     --no-scylla-helper-slice  skip configuring systemd scylla-helper.slice
     --no-enable-service       skip enabling service
     -h, --help                print this help

   Interactive mode is enabled when no flags are provided.

Run the ``scyllamgr_agent_setup`` script to configure the service. You will need to run this command as root or with sudo.

For example:

.. code-block:: none

   sudo scyllamgr_agent_setup -y

.. _configure-auth-token:

Configure an authentication token
=================================

Authentication tokens are used to authenticate requests to the Agent.
Unauthenticated requests are rejected.
**Use the same token on all the nodes in a cluster**. Use different tokens in different clusters.

**Procedure**

#. On **one node only** generate an authentication token to be used to authenticate ScyllaDB Manager with the Agent.
   Run the token generator script. For example:

   .. code-block:: none

      scyllamgr_auth_token_gen
      6Es3dm24U72NzAu9ANWmU3C4ALyVZhwwPZZPWtK10eYGHJ24wMoh9SQxRZEluWMc0qDrsWCCshvfhk9uewOimQS2x5yNTYUEoIkO1VpSmTFu5fsFyoDgEkmNrCJpXtfM

#. Take the auth token you generated, and place it into ``/etc/scylla-manager-agent/scylla-manager-agent.yaml`` as the ``auth_token`` parameter value, remember to uncomment the line. For Example:

   .. code-block:: none

      cat /etc/scylla-manager-agent/scylla-manager-agent.yaml

   Results in:

   .. code-block:: none

      # Scylla Manager Agent config YAML

      # Specify authentication token, the auth_token needs to be the same for all the
      # nodes in a cluster. Use scyllamgr_auth_token_gen to generate the auth_token
      # value.
      auth_token: 6Es3dm24U72NzAu9ANWmU3C4ALyVZhwwPZZPWtK10eYGHJ24wMoh9SQxRZEluWMc0qDrsWCCshvfhk9uewOimQS2x5yNTYUEoIkO1VpSmTFu5fsFyoDgEkmNrCJpXtfM

      ...

   .. note:: Use the same token on all the nodes in a cluster

Start ScyllaDB Manager Agent service
=====================================

**Procedure**

#. Start ScyllaDB Manager Agent service.

   .. code-block:: none

      sudo systemctl start scylla-manager-agent

#. Verify the ScyllaDB Manager Agent is running.

   .. code-block:: none

      sudo systemctl status scylla-manager-agent -l
      ● scylla-manager-agent.service - Scylla Manager Agent
        Loaded: loaded (/usr/lib/systemd/system/scylla-manager-agent.service; disabled; vendor preset: disabled)
        Active: active (running) since Wed 2019-10-30 10:46:51 UTC; 7s ago
          Main PID: 14670 (scylla-manager-)
          CGroup: /system.slice/scylla-manager-agent.service
                 └─14670 /usr/bin/scylla-manager-agent

Next steps
==========

* :ref:`Configure backup location <backup-location>`
* :ref:`Configure native backup <configure-native-backup-in-scylla>`
* :doc:`Add a Cluster <add-a-cluster>`
