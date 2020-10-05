=============
Add a Cluster
=============

.. contents::
   :depth: 2
   :local:

Scylla Manager manages clusters. A cluster contains one or more nodes / datacenters. When you add a cluster to Scylla Manager, it adds all of the nodes which are:

* associated with it, 
* that are running Scylla Manager Agent, 
* and are accessible   


Port Settings
=============

Confirm all ports required for Scylla Manager and Scylla Manager Agent are open. This includes:

* 9042 CQL
* 9142 SSL CQL
* 10001 Scylla Agent REST API

: _add-cluster:

Add a Cluster
=============

This procedure adds the nodes to Scylla Manager so the cluster can be a managed cluster under Scylla Manager.

Prerequisites
-------------

For each node in the cluster, the **same** `authentication token <../install-agent/#generate-an-authentication-token>`_ needs to be identified in ``/etc/scylla-manager-agent/scylla-manager-agent.yaml``

Create a Managed Cluster
------------------------

.. _name:

**Procedure**

#. From the Scylla Manager Server, provide the broadcast_address of one of the nodes and the generated auth_token (if used) and a custom name if desired.

   Where:

   * ``--host`` is hostname or IP of one of the cluster nodes. You can use an IPv6 or an IPv4 address.
   * ``--name`` is an alias you can give to your cluster. Using an alias means you do not need to use the ID of the cluster in all other operations.  
   * ``--auth-token`` is the authentication `token <../install-agent/#generate-an-authentication-token>`_ you identified in ``/etc/scylla-manager-agent/scylla-manager-agent.yaml``
   * ``--without-repair`` - when cluster is added, Manager schedules repair to repeat every 7 days. To create a cluster without a scheduled repair, use this flag.
   * ``--username`` and ``--password`` - optionally you can provide CQL credentials to the cluster.
     For security reasons the user should NOT have access to your data.
     This enables `CQL query based health check <../health-check/#cql-query-health-check>`_ compared to `credentials agnostic health check <../health-check/#credentials-agnostic-health-check>`_ if you do not specify the credentials.
     This also enables CQL schema backup, which isn't performed if credentials aren't provided.

   Example (IPv4):

   .. code-block:: none

      sctool cluster add --host 34.203.122.52 --auth-token "6Es3dm24U72NzAu9ANWmU3C4ALyVZhwwPZZPWtK10eYGHJ24wMoh9SQxRZEluWMc0qDrsWCCshvfhk9uewOimQS2x5yNTYUEoIkO1VpSmTFu5fsFyoDgEkmNrCJpXtfM" --name prod-cluster

      c1bbabf3-cad1-4a59-ab8f-84e2a73b623f
       __  
      /  \     Cluster added! You can set it as default, by exporting env variable.
      @  @     $ export SCYLLA_MANAGER_CLUSTER=c1bbabf3-cad1-4a59-ab8f-84e2a73b623f
      |  |     $ export SCYLLA_MANAGER_CLUSTER=prod-cluster
      || |/    
      || ||    Now run:
      |\_/|    $ sctool status -c prod-cluster
      \___/    $ sctool task list -c prod-cluster


   Example (IPv6):

   .. code-block:: none

         sctool cluster add --host 2a05:d018:223:f00:971d:14af:6418:fe2d --auth-token       "6Es3dm24U72NzAu9ANWmU3C4ALyVZhwwPZZPWtK10eYGHJ24wMoh9SQxRZEluWMc0qDrsWCCshvfhk9uewOimQS2x5yNTYUEoIkO1VpSmTFu5fsFyoDgEkmNrCJpXtfM" --name prod-cluster

   Each cluster has a unique ID.
   You will use this ID in all commands where the cluster ID is required.
   Each cluster is automatically registered with a repair task which runs once a week.
   This can be canceled using ``--without-repair``.
   To use a different repair schedule, see `Schedule a Repair <../repair/#schedule-a-repair>`_.

#. Verify that the cluster you added has a registered repair task by running the ``sctool task list -c <cluster-name>`` command, adding the name_  of the cluster you created in step 1 (with the ``--name`` flag).

   .. code-block:: none

      sctool task list -c prod-cluster
      ╭─────────────────────────────────────────────────────────────┬───────────┬────────────────────────────────┬────────╮
      │ Task                                                        │ Arguments │ Next run                       │ Status │
      ├─────────────────────────────────────────────────────────────┼───────────┼────────────────────────────────┼────────┤
      │ healthcheck/8988932e-de2f-4c42-a2f8-ae3b97fd7126            │           │ 02 Apr 20 12:28:10 CEST (+15s) │ NEW    │
      | healthcheck_alternator/79170f1f-8bda-481e-8538-c3ff9894d235 │           │ 02 Apr 20 12:28:10 CEST (+15s) │ NEW    │
      │ healthcheck_rest/9b7e694d-a1e3-42f1-8ca6-d3dfd9f0d94f       │           │ 02 Apr 20 12:28:40 CEST (+1h)  │ NEW    │
      │ repair/0fd8a43b-eacf-4df8-9376-2a31b0dee6cc                 │           │ 03 Apr 20 00:00:00 CEST (+7d)  │ NEW    │
      ╰─────────────────────────────────────────────────────────────┴───────────┴────────────────────────────────┴────────╯

   You will see 4 tasks which are created by adding the cluster:

   .. include:: _common/health-check-tasks.rst
   * Repair - an automated repair task, starting at midnight tonight, repeating every seven days at midnight. See `Run a Repair <../repair/>`_

   .. note:: If you want to change the schedule for the repair, see `Reschedule a repair <../repair/#reschedule-a-repair>`_.

Connect Managed Cluster to Scylla Monitoring
============================================

Connecting your cluster to Scylla Monitoring allows you to see metrics about your cluster and Scylla Manager all within Scylla Monitoring. 

To connect your cluster to Scylla Monitoring it is **required** to use the same cluster name_ as you used when you created the cluster. See :ref:`add-cluster`.

**Procedure**

Follow the procedure `Scylla Monitoring <http://scylladb.github.io/scylla-monitoring/master/monitoring_stack.html#install-scylla-monitoring>` as directed, remembering to update the Scylla Node IPs and  Cluster name_  as well as the Scylla Manager IP in the relevant Prometheus configuration files.

If you have any issues connecting to Scylla Monitoring Stack consult the `Troubleshooting Guide <https://docs.scylladb.com/troubleshooting/manager_monitoring_integration/>`_.

Add a Node to a Managed Cluster
===============================

Although Scylla Manager is aware of all topology changes made within every cluster it manages, it cannot properly manage nodes/datacenters without establishing connections with every node/datacenter in the cluster including the Scylla Manager Agent which is on each managed node. 

**Before You Begin**

* Confirm you have a managed cluster running under Scylla Manager. If you do not have a managed cluster, see :ref:`add-cluster`.
* Confirm the `node <https://docs.scylladb.com/operating-scylla/procedures/cluster-management/add_node_to_cluster/#procedure>`_ or `Datacenter </operating-scylla/procedures/cluster-management/add_dc_to_existing_dc/#procedure>`_ is added to the Scylla Cluster.

**Procedure**

#. `Add Scylla Manager Agent <../install-agent>`_ to the new node. Use the **same** authentication token as you did for the other nodes in this cluster. Do not generate a new token. 

#. Confirm the node / datacenter was added by checking its `status <../sctool/#status>`_. From the node running Scylla Manager server run the ``sctool status`` command, using the name of the managed cluster.
 
   .. code-block:: none
   
      sctool status -c prod-cluster
      Datacenter: eu-west
      ╭────┬────────────┬───────────┬───────────┬───────────────┬──────────┬──────┬──────────┬────────┬──────────┬──────────────────────────────────────╮
      │    │ Alternator │ CQL       │ REST      │ Address       │ Uptime   │ CPUs │ Memory   │ Scylla │ Agent    │ Host ID                              │
      ├────┼────────────┼───────────┼───────────┼───────────────┼──────────┼──────┼──────────┼────────┼──────────┼──────────────────────────────────────┤
      │ UN │ UP (4ms)   │ UP (3ms)  │ UP (2ms)  │ 34.203.122.52 │ 237h2m1s │ 4    │ 15.43GiB │ 4.1.0  │ 2.2.0    │ 8bfd18f1-ac3b-4694-bcba-30bc272554df │
      │ UN │ UP (15ms)  │ UP (11ms) │ UP (12ms) │ 10.0.138.46   │ 237h2m1s │ 4    │ 15.43GiB │ 4.1.0  │ 2.2.0    │ 238acd01-813c-4c55-bd65-5219bb19bc20 │
      │ UN │ UP (17ms)  │ UP (5ms)  │ UP (7ms)  │ 10.0.196.204  │ 237h2m1s │ 4    │ 15.43GiB │ 4.1.0  │ 2.2.0    │ bde4581a-b25e-49fc-8cd9-1651d7683f80 │
      │ UN │ UP (10ms)  │ UP (4ms)  │ UP (5ms)  │ 10.0.66.115   │ 237h2m1s │ 4    │ 15.43GiB │ 4.1.0  │ 2.2.0    │ 918a52aa-cc42-43a4-a499-f7b1ccb53b18 │
      ╰────┴────────────┴───────────┴───────────┴───────────────┴──────────┴──────┴──────────┴────────┴──────────┴──────────────────────────────────────╯


#. If you are using the Scylla Monitoring Stack, continue to `Connect Managed Cluster to Scylla Monitoring`_ for more information. 

Remove a Node/Datacenter from Scylla Manager
--------------------------------------------

There is no need to perform any action in Scylla Manager after removing a node or datacenter from a Scylla cluster. 

.. note:: If you are removing the cluster from Scylla Manager and you are using Scylla Monitoring, refer to `targets example <http://scylladb.github.io/scylla-monitoring/master/monitoring_stack.html#configure-scylla-nodes-from-files>`_ for more information.

See Also
========

* `sctool Reference <../sctool>`_
* `Remove a node from a Scylla Cluster </operating-scylla/procedures/cluster-management/remove_node>`_ 
* `Scylla Monitoring </operating-scylla/monitoring>`_

