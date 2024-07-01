===========================
Setup S3 compatible storage
===========================

.. contents::
   :depth: 2
   :local:

There are multiple S3 API compatible providers that can be used with Scylla Manager.
Due to minor differences between them we require that exact provider is specified in the config file for full compatibility.
The available providers are Alibaba, AWS, Ceph, DigitalOcean, IBMCOS, Minio, Wasabi, Dreamhost, Netease.

Create a bucket
===============

You need to create a bucket in your storage system of choice.

Grant access
============

This procedure is required so that Scylla Manager can access your bucket.
You need to configure bucket access policy in your storage system and set credentials in the Scylla Manager Agent config file.

Policy
------

The user must have list, read, write and delete privileges to the bucket, for specifics consult :ref:`the sample AWS IAM policy <aws-iam-policy>`.

MinIO Example
.............

With MinIO you can use the the same policy files as with AWS.

**Procedure**

Given `myminio` is an alias for your MinIO deployment.

#. Create a user by providing access and secret keys.

   .. code-block:: none

      mc admin user add myminio "${MINIO_USER_ACCESS_KEY}" "${MINIO_USER_SECRET_KEY}"

#. Copy :ref:`the sample AWS IAM policy <aws-iam-policy>` as ``user-policy.json``.

#. Replace ``scylla-manager-backup`` in ``user-policy.json`` with your bucket name.

#. Create a user policy.

   .. code-block:: none

      mc admin policy add myminio sm-user-policy user-policy.json

#. Attach the policy to the user.

   .. code-block:: none

      mc admin policy set myminio sm-user-policy user="${MINIO_USER_ACCESS_KEY}"

Config file
-----------

Note that this procedure needs to be repeated for each Scylla node.

**Procedure**

Edit the ``/etc/scylla-manager-agent/scylla-manager-agent.yaml``

#. Uncomment the ``s3:`` line, for parameters note the two spaces in front, it's a yaml file.
#. Uncomment and set ``access_key_id`` and ``secret_access_key``.
#. Uncomment and set ``provider`` to one of the supported providers.
#. Uncomment and set ``endpoint`` to the base URL of your storage.
#. Validate that the manager has access to the backup location.
   If there is no response, the S3 bucket is accessible. If not, you will see an error.

   .. code-block:: none

      scylla-manager-agent check-location --location s3:<your S3 bucket name>

MinIO Example
.............

.. code-block:: yaml

   s3:
     access_key_id: AKIAIOSFODNN7EXAMPLE
     secret_access_key: wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
     provider: Minio
     endpoint: http://192.168.121.99:9000

Troubleshoot connectivity
=========================

To troubleshoot Node to bucket connectivity issues you can run:

.. code-block:: none

   scylla-manager-agent check-location --debug --location s3:<your S3 bucket name>
