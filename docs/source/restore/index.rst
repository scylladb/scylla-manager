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

.. note:: make sure to update your ``var.yaml`` so the IPs of the **destination cluster** map to UUIDs of nodes of the **source cluster**.


For example, for a source couster

.. code::

   > nodetool status
   
   Datacenter: datacenter1
   =======================
   Status=Up/Down
   |/ State=Normal/Leaving/Joining/Moving
   --  Address      Load       Tokens       Owns    Host ID                               Rack
   UN  10.240.0.6   119.6 MB   256          ?       9b6b5206-63c8-4cdf-9b4f-23e37977fd14  rack1
   UN  10.240.0.93  122.69 MB  256          ?       2d8045e7-51a4-42c4-bb8f-5e8aa77a8228  rack1

and a Destination cluster:

.. code::

   > nodetool status
   
   Datacenter: us-east1
   ====================
   Status=Up/Down
   |/ State=Normal/Leaving/Joining/Moving
   --  Address        Load       Tokens       Owns    Host ID                               Rack
   UN  75.196.92.133  122.14 MB  256          ?       a21b38b4-c0b1-49f0-89f2-931fe027d6b0  b
   UN  75.237.179.49  121.22 MB  256          ?       a0a63682-95e3-4cf3-9f46-4d551bd7eba2  b


``vars.yaml`` file would look like:

.. code::

   # backup_location specifies the location parameter used in Scylla Manager
   # when scheduling a backup of a cluster.
   backup_location: gcs:manager-bucket 

   # snapshot_tag specifies the Scylla Manager snapshot tag you want to restore.
   snapshot_tag: sm_20220204002134UTC

   # host_id specifies a mapping from the clone cluster node IP to the source
   # cluster host IDs.
   host_id:
     75.237.179.49: 9b6b5206-63c8-4cdf-9b4f-23e37977fd14
     75.196.92.133: 2d8045e7-51a4-42c4-bb8f-5e8aa77a8228
   # destination IP: source ID

The ``hosts`` file would look like:

.. code::

   75.237.179.49
   75.196.92.133
   

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
