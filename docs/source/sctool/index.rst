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


``sctool`` is a Command Line Interface (CLI) for the Scylla Manager server.
The server communicates with managed Scylla clusters and performs cluster-wide operations such as automatic repair and backup.

Syntax
---------

.. code-block:: none

   sctool <command> [flags] [global flags]

.. note:: Flags that have default values must be set with ``=``.

See the left pane menu for commands.

Examples
----------

.. code-block:: console

   sctool tasks -c mycluster --show-properties=true


.. code-block:: console

   sctool repair update -c mycluster repair/aaaa1111-bb22-cc33-dd44-eeeeee555555 --enabled=false


.. code-block:: console

   scylla-manager-agent download-files --location s3:my-bucket -p=10


