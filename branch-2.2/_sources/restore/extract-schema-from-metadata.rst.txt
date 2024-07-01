============================
Extract schema from metadata
============================

**Prerequisites**

#. You have the snapshot tag.
#. The snapshot contains ``schema.tar.gz`` file.

**Procedure**

#. List files located in snapshot you want to restore.

   .. code-block:: none

      sctool backup files -c my-cluster -L s3:backups -T sm_20191210145143UTC

   The first output line is a path to schemas archive, for example:

   .. code-block:: none

      s3://backups/backup/schema/cluster/ed63b474-2c05-4f4f-b084-94541dd86e7a/task_287791d9-c257-4850-aef5-7537d6e69d90_tag_sm_20200506115612UTC_schema.tar.gz      ./

#. Download schema archive.

   .. code-block:: none

      aws s3 cp s3://backups/backup/schema/cluster/ed63b474-2c05-4f4f-b084-94541dd86e7a/task_287791d9-c257-4850-aef5-7537d6e69d90_tag_sm_20200506115612UTC_schema.tar.gz ./

#. Extract CQL files from archive. This archive contains a single CQL file for each keyspace in the backup.

   .. code-block:: none

      tar -xzvf task_287791d9-c257-4850-aef5-7537d6e69d90_tag_sm_20200506115612UTC_schema.tar.gz
