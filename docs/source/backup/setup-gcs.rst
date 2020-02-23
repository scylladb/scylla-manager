==========================
Setup Google Cloud Storage
==========================

.. contents::
   :depth: 2
   :local:

Create a bucket
===============

Go to `Google Cloud Storage <https://cloud.google.com/storage>`_ and create a new bucket in a region where Scylla nodes are.
If your cluster is deployed in multiple regions create a bucket per region.
You may decide to backup only a single datacenter to save on costs, in that case create only one bucket in a region you want to backup.

Grant access
============

This procedure is required so that Scylla Manager can access your bucket.

Choose how you want to configure access to the bucket.
If your application runs inside a Google Cloud environment we recommend using automatic Service account authentication.
Otherwise you can add your credentials to the agent configuration file.
The later method is less secure as you will be propagating each node with this security information and in cases where you need to change the key, you will have to replace it on each node.

Automatic service account authorization
---------------------------------------

**Procedure**

#. Collect list of `service accounts <https://cloud.google.com/compute/docs/access/service-accounts>`_ used by **each** of the Scylla nodes.
#. Make sure that each of service account has read/write `access scope <https://cloud.google.com/compute/docs/access/service-accounts#accesscopesiam>`_ to Cloud Storage.
#. For each service account from the list, add `Storage Object Admin role <https://cloud.google.com/storage/docs/access-control/iam-roles>`_ in bucket permissions settings.

Service account file
--------------------

Note that this procedure needs to be repeated for each Scylla node.

**Prerequisites**

Use `this instruction <https://cloud.google.com/docs/authentication/production#manually>`_ to get the service account file.

**Procedure**

#. Upload service account file to ``/etc/scylla-manager-agent/gcs-service-account.json``.
   If you want to use different path change service_account_file parameter in ``gcs`` section in :doc:`Scylla Manager Agent Config file <../config/scylla-manager-agent-config>`.
#. Validate that the manager has access to the backup location.
   If there is no response, the bucket is accessible. If not, you will see an error.

   .. code-block:: none

      scylla-manager-agent check-location --location gcs:<your GCS bucket name>

Troubleshoot connectivity
=========================

To troubleshoot Node to bucket connectivity issues you can run:

.. code-block:: none

   scylla-manager-agent check-location --debug --location gcs:<your GCS bucket name>
