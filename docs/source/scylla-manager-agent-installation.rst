.. _install-agent:

=================================
Scylla Manager Agent Installation
=================================

Scylla Manager Agent is a daemon, that needs to be **installed and started on each Scylla node**.
Scylla Manager Server communicates with nodes in the managed Scylla clusters via the Agents.
The communication is encrypted (HTTPS) and protected by an auth token.
Agent serves as a reverse proxy to Scylla REST API, and provides additional features specific to Scylla Manager.

.. note:: Repeat the procedure for **every** Scylla node in the cluster that you want to be managed by Scylla Manager.

.. contents::
   :depth: 2
   :local:

Install package
===============

Download and install Scylla Manager Agent from the `Scylla Download Center <https://www.scylladb.com/download/#manager>`_.

Run the scyllamgr_agent_setup script
====================================

The Scylla Manager Agent setup script automates configuration of Scylla Manager Agent by asking you some questions.
It can be run in non-interactive mode by using flags.

.. code-block:: none

   scyllamgr_agent_setup -h
   Usage: scyllamgr_agent_setup [-y][--no-scylla-helper-slice]

   Options:
     -y, --assume-yes          assume that the answer to any question which would be asked is yes
     --no-scylla-helper-slice  skip configuring systemd scylla-helper.slice
     --no-enable-service       skip enabling service
     -h, --help                print this help

   Interactive mode is enabled when no flags are provided.

Run the ``scyllamgr_agent_setup`` script to configure the service.

.. _configure-auth-token:

Configure an authentication token
=================================

Authentication tokens are used to authenticate requests to the Agent.
Unauthenticated requests are rejected.
**Use the same token on all the nodes in a cluster**. Use different tokens in different clusters.

**Procedure**

#. On **one node only** generate an authentication token to be used to authenticate Scylla Manager with the Agent.
   Run the token generator script. For example:

   .. code-block:: none

      scyllamgr_auth_token_gen
      6Es3dm24U72NzAu9ANWmU3C4ALyVZhwwPZZPWtK10eYGHJ24wMoh9SQxRZEluWMc0qDrsWCCshvfhk9uewOimQS2x5yNTYUEoIkO1VpSmTFu5fsFyoDgEkmNrCJpXtfM

#. Take the auth token you generated, and place it into ``/etc/scylla-manager-agent/scylla-manager-agent.yaml`` as the ``auth_token`` parameter value, remember to uncomment the line. For Example:

   .. code-block:: none

      $ cat /etc/scylla-manager-agent/scylla-manager-agent.yaml
      # Scylla Manager Agent config YAML

      # Specify authentication token, the auth_token needs to be the same for all the
      # nodes in a cluster. Use scyllamgr_auth_token_gen to generate the auth_token
      # value.
      auth_token: 6Es3dm24U72NzAu9ANWmU3C4ALyVZhwwPZZPWtK10eYGHJ24wMoh9SQxRZEluWMc0qDrsWCCshvfhk9uewOimQS2x5yNTYUEoIkO1VpSmTFu5fsFyoDgEkmNrCJpXtfM

   .. note:: Use the same token on all the nodes in a cluster

Start Scylla Manager Agent service
==================================

**Procedure**

#. Start Scylla Manager Agent service.

   .. code-block:: none

      sudo systemctl start scylla-manager-agent

#. Verify the Scylla Manager Agent is running.

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

..
   TODO add link to backup configuration

* :ref:`Add a Cluster <add-cluster>`.
