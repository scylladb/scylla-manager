================
Repair a Cluster
================

.. toctree::
   :hidden:
   :maxdepth: 2

   examples
   change-repair-speed


Repair is important to make sure that data across the nodes is consistent.
This is the last resort of defence with an eventually consistent database.
When you create a cluster a repair job is automatically scheduled.
This task is set to occur each week by default, but you can change it to another time, change its parameters or add additional repair tasks if you need.

Benefits of using Scylla Manager repairs
========================================

Scylla Manager automates the repair process and allows you to configure how and when repair occurs.
The advantages of using Scylla Manager for repair operations are:

* Data selection - repair a single table or an entire cluster, the choice is up to you
* Control - user controls the degree of parallelism, and intensity of a repair, the parameters may be modified on the flight
* Fast by default - Scylla Manager chooses the optimal number of parallel repairs supported by the cluster by default
* In sync with node - intensity may be automatically adjusted to the maximum supported by a node
* Pause and resume - repair can be paused and resumed later, it will continue where it left off
* Retries - retries in case of transport (HTTP) errors, and retries of repairs of failed token ranges
* Schema changes - Scylla Manager handles adding and removing tables and keyspaces during repair
* Visibility - everything is managed from one place, progress can be read using CLI, REST API or Prometheus metrics, you can dig into details and get to know progress of individual tables and nodes
