.. _sctool-reference-index:

==========
CLI sctool
==========

.. toctree::
   :hidden:
   :maxdepth: 2

   global-flags-and-variables
   completion
   download-files
   backup
   restore
   cluster
   info
   progress
   repair
   start
   status
   stop
   suspend-resume
   task
   version


``sctool`` is a Command Line Interface (CLI) for the ScyllaDB Manager server.
The server communicates with managed ScyllaDB clusters and performs cluster-wide operations such as automatic repair and backup.

Syntax
---------

.. code-block:: none

   sctool <command> [flags] [global flags]

.. note:: Flags that have default values must be set with ``=``.

See the left pane menu for commands.

Examples
----------

List tasks with set properties
..............................

  .. code-block:: console

    sctool tasks -c prod-cluster --show-properties
    ╭─────────────────────────────────────────────┬──────────────┬────────┬───────────────┬─────────┬───────┬─────────────────────────┬────────────┬─────────┬─────────────────────────┬───────────────────────────╮
    │ Task                                        │ Schedule     │ Window │ Timezone      │ Success │ Error │ Last Success            │ Last Error │ Status  │ Next                    │ Properties                │
    ├─────────────────────────────────────────────┼──────────────┼────────┼───────────────┼─────────┼───────┼─────────────────────────┼────────────┼─────────┼─────────────────────────┼───────────────────────────┤
    │ healthcheck/cql                             │ @every 15s   │        │ Europe/Warsaw │ 2       │ 0     │ 08 Sep 23 11:05:07 CEST │            │ DONE    │ 08 Sep 23 11:05:22 CEST │ mode: cql                 │
    │ healthcheck/rest                            │ @every 1m0s  │        │ Europe/Warsaw │ 0       │ 0     │                         │            │ NEW     │ 08 Sep 23 11:05:37 CEST │ mode: rest                │
    │ healthcheck/alternator                      │ @every 15s   │        │ Europe/Warsaw │ 2       │ 0     │ 08 Sep 23 11:05:07 CEST │            │ DONE    │ 08 Sep 23 11:05:22 CEST │ mode: alternator          │
    │ repair/all-weekly                           │ 0 23 * * SAT │        │ Europe/Warsaw │ 0       │ 0     │                         │            │ NEW     │ 09 Sep 23 23:00:00 CEST │                           │
    │ repair/e8552c59-dfd3-4b2e-ab58-8920b6d0662c │              │        │ Europe/Warsaw │ 0       │ 0     │                         │            │ RUNNING │                         │ intensity: 5, parallel: 7 │
    ╰─────────────────────────────────────────────┴──────────────┴────────┴───────────────┴─────────┴───────┴─────────────────────────┴────────────┴─────────┴─────────────────────────┴───────────────────────────╯

Disable repair task
...................

  .. code-block:: console

    sctool repair update -c prod-cluster repair/all-weekly --enabled=false

List also the disabled tasks
............................

Note that disabled tasks are prefixed with ``*``.

  .. code-block:: console

    sctool tasks -c prod-cluster -a
    ╭─────────────────────────────────────────────┬──────────────┬────────┬───────────────┬─────────┬───────┬─────────────────────────┬────────────┬────────┬─────────────────────────╮
    │ Task                                        │ Schedule     │ Window │ Timezone      │ Success │ Error │ Last Success            │ Last Error │ Status │ Next                    │
    ├─────────────────────────────────────────────┼──────────────┼────────┼───────────────┼─────────┼───────┼─────────────────────────┼────────────┼────────┼─────────────────────────┤
    │ healthcheck/cql                             │ @every 15s   │        │ Europe/Warsaw │ 288     │ 0     │ 08 Sep 23 12:16:37 CEST │            │ DONE   │ 08 Sep 23 12:16:52 CEST │
    │ healthcheck/rest                            │ @every 1m0s  │        │ Europe/Warsaw │ 72      │ 0     │ 08 Sep 23 12:16:37 CEST │            │ DONE   │ 08 Sep 23 12:17:37 CEST │
    │ healthcheck/alternator                      │ @every 15s   │        │ Europe/Warsaw │ 288     │ 0     │ 08 Sep 23 12:16:37 CEST │            │ DONE   │ 08 Sep 23 12:16:52 CEST │
    │ *repair/all-weekly                          │ 0 23 * * SAT │        │ Europe/Warsaw │ 0       │ 0     │                         │            │ NEW    │                         │
    │ repair/e8552c59-dfd3-4b2e-ab58-8920b6d0662c │              │        │ Europe/Warsaw │ 1       │ 0     │ 08 Sep 23 11:05:56 CEST │            │ DONE   │                         │
    ╰─────────────────────────────────────────────┴──────────────┴────────┴───────────────┴─────────┴───────┴─────────────────────────┴────────────┴────────┴─────────────────────────╯

Enable repair task
..................

  .. code-block:: console

    sctool repair update -c prod-cluster repair/all-weekly --enabled=true

Download files from backup location
...................................

  .. code-block:: console

    scylla-manager-agent download-files --location s3:my-bucket -p=10


