Global flags and variables
--------------------------

The flags and variables presented in this section can be used with any sctool command.

.. _global-flags:

Global flags
============

* ``--api-cert-file <path>`` - specifies the path to HTTPS client certificate used to access the Scylla Manager server
* ``--api-key-file <path>`` - specifies the path to HTTPS client key used to access the Scylla Manager server
* ``--api-url URL`` - URL of Scylla Manager server (default "http://127.0.0.1:5080/api/v1")
* ``-c, --cluster <cluster_name>`` - Specifies the target cluster name or ID
* ``-h, --help`` - Displays help for commands. Use ``sctool [command] --help`` for help about a specific command.

Environment variables
=====================

sctool uses the following environment variables:

* `SCYLLA_MANAGER_CLUSTER` - if set, specifies the default value for the ``-c, --cluster`` flag, in commands that support it.
* `SCYLLA_MANAGER_API_URL` - if set, specifies the default value for the ``--api-url`` flag; it can be useful when using sctool with a remote Scylla Manager server.

The environment variables may be saved in  your ``~/.bashrc`` file so that the variables are set after login.
