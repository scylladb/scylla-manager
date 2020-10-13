.. _sctool-reference-index:

======================
sctool Reference Guide
======================

.. toctree::
   :hidden:
   :maxdepth: 2

   global-flags-and-variables
   cluster
   backup
   repair
   status
   task
   version


``sctool`` is a Command Line Interface (CLI) for the Scylla Manager server.
The server communicates with managed Scylla clusters and performs cluster-wide operations such as automatic repair and backup.

**Usage**

.. code-block:: none

   sctool command [flags] [global flags]

Select a topic to begin:

* :doc:`sctool global flags and variables <global-flags-and-variables>`
* :doc:`Cluster registration <cluster>`
* :doc:`Backup a cluster <backup>`
* :doc:`Repair a cluster <repair>`
* :doc:`Monitor a cluster <status>`
* :doc:`Manage tasks <task>`
* :doc:`Show Scylla Manager version <version>`

