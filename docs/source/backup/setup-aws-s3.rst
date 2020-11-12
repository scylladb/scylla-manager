============
Setup AWS S3
============

.. contents::
   :depth: 2
   :local:

Create a bucket
===============

Go to `Amazon S3 <https://aws.amazon.com/s3/>`_ and create a new bucket in a region where Scylla nodes are.
If your cluster is deployed in multiple regions create a bucket per region.
You may decide to backup only a single datacenter to save on costs, in that case create only one bucket in a region you want to backup.

Grant access
============

This procedure is required so that Scylla Manager can access your bucket.

Choose how you want to configure access to the bucket.
You can use an IAM role (recommended) or you can add your credentials to the agent configuration file.
The later method is less secure as you will be propagating each node with this security information and in cases where you need to change the key, you will have to replace it on each node.

IAM role
--------

**Procedure**

#. Create an `IAM role <https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/iam-roles-for-amazon-ec2.html>`_ for the S3 bucket which adheres to your company security policy.
#. `Attach the IAM role <https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/iam-roles-for-amazon-ec2.html#attach-iam-role>`_ to **each EC2 instance (node)** in the cluster.

.. _aws-iam-policy:

Sample IAM policy for *scylla-manager-backup* bucket:

.. code-block:: none

   {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": [
                    "s3:GetBucketLocation",
                    "s3:ListBucket",
                    "s3:ListBucketMultipartUploads"
                ],
                "Resource": [
                    "arn:aws:s3:::scylla-manager-backup"
                ]
            },
            {
                "Effect": "Allow",
                "Action": [
                    "s3:PutObject",
                    "s3:GetObject",
                    "s3:DeleteObject",
                    "s3:AbortMultipartUpload",
                    "s3:ListMultipartUploadParts"
                ],
                "Resource": [
                    "arn:aws:s3:::scylla-manager-backup/*"
                ]
            }
        ]
   }

Config file
-----------

Note that this procedure needs to be repeated for each Scylla node.

**Procedure**

Edit the ``/etc/scylla-manager-agent/scylla-manager-agent.yaml``

#. Uncomment the ``s3:`` line, for parameters note the two spaces in front, it's a yaml file.
#. Uncomment and set ``access_key_id`` and ``secret_access_key``.
#. If the S3 bucket is **not** running in the **same region** as the AWS EC2 instance uncomment and set the ``region`` to the S3 bucket's region.

#. Validate that the manager has access to the backup location.
   If there is no response, the S3 bucket is accessible. If not, you will see an error.

   .. code-block:: none

      scylla-manager-agent check-location --location s3:<your S3 bucket name>

Additional features
====================

You can enable additional AWS S3 features such as **server side encryption** or **transfer acceleration**.
Those need to be enabled on per Agent basis in the configuration file.
Check out the ``s3`` section in :doc:`Scylla Manager Agent Config file <../config/scylla-manager-agent-config>`.

Troubleshoot connectivity
=========================

To troubleshoot Scylla node to bucket connectivity issues you can run:

.. code-block:: none

   scylla-manager-agent check-location --debug --location s3:<your S3 bucket name>