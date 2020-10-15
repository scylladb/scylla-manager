==============================================
Grant Access Control to your Amazon S3 Bucket
==============================================

This procedure is required so that Scylla Manager can access your Amazon S3 bucket.

**Procedure**

#. Create an `IAM role <https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/iam-roles-for-amazon-ec2.html>`_ for the S3 bucket which adheres to your company security policy.
#. `Attach the IAM role <https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/iam-roles-for-amazon-ec2.html#attach-iam-role>`_ to **each EC2 instance (node)** in the cluster.

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

**To add your AWS credentials the Scylla Manager Agent configuration file**

Edit the ``/etc/scylla-manager-agent/scylla-manager-agent.yaml``

#. Uncomment the ``s3:`` line, for parameters note the two spaces in front, it's a yaml file.
#. Uncomment and set ``access_key_id`` and ``secret_access_key``, refer to :ref:`Scylla Agent Configuration <configure-auth-token>` for details.
#. If the S3 bucket is **not** running in the **same region** as the AWS EC2 instance uncomment and set the *region* to the S3 bucket's region.

#. Validate that the manager has access to the backup location.
   If there is no response, the S3 bucket is accessible. If not, you will see an error.

   .. code-block:: none

      $ scylla-manager-agent check-location --location s3:<your S3 bucket name>

Troubleshoot Node to Bucket Connectivity
----------------------------------------

To troubleshoot Node to bucket connectivity issues you can run:

.. code-block:: none

   scylla-manager-agent check-location --debug --location s3:<your S3 bucket name>