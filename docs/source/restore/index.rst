=======
Restore
=======

.. toctree::
   :hidden:
   :maxdepth: 2

   download-files-command

The following procedure lets you restore data to a new cluster to get a cluster clone from snapshots stored in a backup location.

**Prerequisites**

#. New cluster with the same number of nodes as the source cluster.
#. Scylla Manager Agent installed on all the nodes.
#. Access to the backup location from all the nodes.
#. Ansible installed on your local machine.
#. Access to all the nodes over SSH from your local machine.

If you do not know the exact number of nodes of the source cluster, you can create a single node cluster and discover the number in the process.
In fact you can use any Scylla node with Scylla Manager Agent installed that has access to the backup location for getting the parameters.
Note that there is no need for Scylla Manager Server installation prior to restore.

Ansible
=======

Cloning the cluster is automated with Ansible playbook.
The playbook has a `readme <https://github.com/scylladb/scylla-manager/tree/master/ansible/restore>`_ that explains how to use it.
For reader's convenience it's repeated here.

Getting the playbook
--------------------

**Procedure**

#. Clone repository from GitHub ``git clone git@github.com:scylladb/scylla-manager.git``
#. Go to playbook directory ``cd scylla-manager/ansible/restore``

Playbook parameters
-------------------

All restore parameters shall be put to ``vars.yaml`` file.
Copy ``vars.yaml.example`` as ``vars.yaml`` and change parameters to match your clusters.

IP to host ID mapping
.....................

SSH to one of the nodes and execute the following command:

.. code-block:: none

   scylla-manager-agent download-files -L <backup-location> --list-nodes

it gives you information about all the clusters and nodes available in the backup location.

Example output:

.. code-block:: none

   Cluster: prod (a9dcc6e1-17dc-4520-9e03-0a92011c823c)
   AWS_EU_CENTRAL_1:
   - 18.198.164.180 (7e68421b-acb1-44a7-a1a8-af7eaf1bb482)
   - 3.122.35.120 (adc0a3ce-dade-4672-981e-26f91a3d35cb)
   - 3.127.175.215 (2a575244-3e3c-44a1-a526-da4394f9525e)
   - 3.65.85.108 (82f0f486-370d-4cfd-90ac-46464c8012cb)
   - 3.66.209.145 (9ee92c19-5f78-4865-a287-980218963d96)
   - 3.66.44.239 (aff05f79-7c69-4ecf-a827-5ea790a0fdc6)

   Cluster: test (da5721cd-e2eb-4d10-a3a7-f729b8f72abf)
   AWS_EU_CENTRAL_1:
   - 3.123.55.215 (4001206a-3377-40cb-abd4-d38aad5dec41)
   - 3.127.1.189 (c6466011-02f9-49dd-8951-c32028dfc6f1)
   - 3.64.219.214 (bc39bb07-7a21-41cd-b576-51d44c1a694a)

For each node IP in the new cluster you need to assign a host ID (UUID in the listing above).
The mapping must be put into ``host_id`` variable in ``vars.yaml`` file.

Snapshot tag
............

To list available snapshot tags for a node use ``--list-snapshots`` flag.

.. code-block:: none

   scylla-manager-agent download-files -L <backup-location> --list-snapshots -n <host-id>

You can filter snapshot tags containing a specific keyspaces or tables by using glob patterns.
The parameter ``-K, --keyspace <list of glob patterns to find keyspaces>`` lets you do that.
It works the same way as in scheduling backups or repairs with Scylla Manager.

The snapshot ID must be put into ``snapshot_tag`` variable in ``vars.yaml`` file.

Inventory
.........

Put public IP addresses of all nodes to ``hosts`` file.

Running the playbook
--------------------

Rut the playbook:

.. code-block:: none

   ansible-playbook -i hosts -e @vars.yaml restore.yaml
