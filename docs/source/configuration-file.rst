==================
Configuration file
==================

.. contents::
   :depth: 2
   :local:

Scylla Manager has a single configuration file ``/etc/scylla-manager/scylla-manager.yaml``.
Note that the file will open as read-only unless you edit it as the root user or by using sudo.
Usually there is no need to edit the configuration file.

HTTP/HTTPS server settings
==========================

With server settings you may specify if Scylla Manager should be available over HTTP, HTTPS or both.

.. code-block:: yaml

   # Bind REST API to the specified TCP address using HTTP protocol.
   # http: 127.0.0.1:5080

   # Bind REST API to the specified TCP address using HTTPS protocol.
   https: 127.0.0.1:5443

.. note:: Please note that Scylla Manager 2.2 introduced changes to the ports and ports for http/https were changed from 56080/56443 to 5080/5443.

Prometheus settings
===================

.. code-block:: yaml

   # Bind prometheus API to the specified TCP address using HTTP protocol.
   # By default it binds to all network interfaces but you can restrict it
   # by specifying it like this 127:0.0.1:5090 or any other combination
   # of ip and port.
   prometheus: ':5090'

If changing prometheus IP or port please remember to adjust rules in `prometheus server </operating-scylla/monitoring/monitoring_stack/#procedure>`_.

.. code-block:: yaml

   - targets:
     - IP:5090

.. note:: Please note that Scylla Manager 2.2 introduced changes to the ports and port for Prometheus was changed from 56090 to 5090.

Debug endpoint settings
=======================

In this section, you can specify the pprof debug server address.
It allows you to run profiling on demand on a live application.
By default, the server is running on port ``56112``.

.. code-block:: none

   debug: 127.0.0.1:56112

.. note:: Please note that Scylla Manager 2.2 introduced changes to the ports and port for debug was changed from 56112 to 5112.

Logging settings
================

Logging settings specify log output and level.

.. code-block:: yaml

   # Logging configuration.
   logger:
     # Where to output logs, syslog or stderr.
     mode: stderr
     # Available log levels are error, info and debug.
     level: info

Database settings
=================

Database settings allow for `using a remote cluster <../use-a-remote-db>`_ to store Scylla Manager data.

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
   #
   # Should we use a token aware driver, this would make manager access appropriate
   # host and shard to execute a request eliminating request routing.
   #  token_aware: true

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

Health check settings
=====================

Health check settings let you specify the timeout threshold.
If there is no response from a node after this time period is reached, the `status <../sctool/#status>`_ report (``sctool status``) shows the node as ``DOWN``.

.. code-block:: yaml

   # Healthcheck service configuration.
   #healthcheck:
   # Timeout for CQL status checks.
   #  timeout: 250ms
   #  ssl_timeout: 750ms

In Scylla Manager 2.2 we introduce more options for configuring healthcheck task behavior with dynamic timeouts.

   # Dynamic timeout calculates timeout based past probe measurements.
   # It takes recent probes RTTs, calculates mean (m) and standard
   # deviation (stddev) and returns timeout of next probe
   # equal to m + stddev_multiplier * stddev.
   # Higher stddev_multiplier is recommended on stable network environments, because standard
   # deviation may be close to 0.
   #  dynamic_timeout:
   #    enabled: true
   #    probes: 200
   #    max_timeout: 30s
   #    stddev_multiplier: 5

Backup settings
===============

Backup settings let you specify backup parameters.

.. code-block:: yaml

   # Backup service configuration.
   #backup:
   # Minimal amount of free disk space required to take a snapshot.
   #  disk_space_free_min_percent: 10
   #
   # Maximal time for backup run to be considered fresh and can be continued from
   # the same snapshot. If exceeded, new run with new snapshot will be created.
   # Zero means no limit.
   #  age_max: 12h

Repair settings
===============

Repair settings let you specify repair parameters.

.. code-block:: yaml

   # Repair service configuration.
   #repair:
   # Frequency Scylla Manager poll Scylla node for repair command status.
   #  poll_interval: 50ms
   #
   # Maximal time a paused repair is considered fresh and can be continued,
   # if exceeded repair will start from the beginning. Zero means no limit.
   #  age_max: 0
   #
   # Specifies how long repair will wait until all ongoing repair requests finish
   # when repair task is stopped. After this time, task will be interrupted.
   #  graceful_stop_timeout: 30s
   #
   # Force usage of certain type of repair algorithm. Allowed values are row_level, legacy, and auto.
   # row_level means that Scylla Manager will use row level repair optimised algorithm.
   # legacy means that Scylla Manager will be splitting ranges to shards when making repair requests.
   # Default value is auto which means that repair type will be auto detected based on Scylla versions in the managed cluster.
   #  force_repair_type: auto
   #
   # Distribution of data among cores (shards) within a node.
   # Copy value from Scylla configuration file.
   #  murmur3_partitioner_ignore_msb_bits: 12
