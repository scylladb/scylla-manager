.. _prepare-nodes:

========================
Prepare Nodes for Backup
========================

#. Create a storage location for the backup.
   Currently, Scylla Manager supports `Amazon S3 buckets <https://aws.amazon.com/s3/>`_ and `Google Cloud Storage buckets <https://cloud.google.com/storage>`_ .
   You can use a bucket that you already created.
   We recommend using an bucket in the same region where your nodes are to minimize cross region data transfer costs.
   In multi-dc deployments you should create a bucket per datacenter, each located in the datacenter's region.
#. Choose how you want to configure access to the bucket.
   You can use an IAM role (recommended) or you can add your credentials to the agent configuration file.
   The later method is less secure as you will be propagating each node with this security information and in cases where you need to change the key, you will have to replace it on each node.

Next Steps
----------
* :doc:`Grant Access Control to your Amazon S3 Bucket <grant-access-S3>`
* :doc:`Grant Access Control to your Google Cloud Storage Bucket <grant-access-gcs>`