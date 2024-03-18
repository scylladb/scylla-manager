==========================================
Extract schema from system_schema keyspace
==========================================

**Prerequisites**

#. You have the snapshot tag.
#. The snapshot contains ``system_schema`` keyspace.

**Procedure**

#. List files of snapshot we want to restore (replace <node_id> with ID of one of the nodes in the cluster.

   .. code::

      sctool backup files --cluster my-cluster -T sm_20200513104924UTC -K system_schema --with-version | grep "<node_id>" > backup_files.out

#. Create a directory, and recreate table directories in it.

   .. code::

      mkdir scylla
      cd scylla
      cat ../backup_files.out | awk '{print $2}' | sort | uniq | xargs mkdir -p

#. Download the SSTables to the created directories

   .. code::

      cat ../backup_files.out | xargs -n2 aws s3 cp
      cd ..

#. Start the Scylla container with mounted data volume.
   Make sure to use the same Scylla version used to create the backup.

   .. code::

      docker run --name scylla -p 9042:9042 -v `pwd`/scylla:/var/lib/scylla/data --memory 1G --rm -d scylladb/scylla:4.3.0

#. Extract schema:

   .. code::

      cqlsh 127.0.0.1 -e "DESCRIBE SCHEMA"  > db_schema.cql
