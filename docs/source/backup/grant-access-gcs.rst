========================================================
Grant Access Control to your Google Cloud Storage Bucket
========================================================

If your application runs inside a Google Cloud environment we recommend using automatic Service Account authentication.

Automatic Service Account Authorization
---------------------------------------

**Procedure**

#. Collect list of `service accounts <https://cloud.google.com/compute/docs/access/service-accounts>`_ used by **each** of the nodes.
#. Make sure that each of service account has read/write `access scope <https://cloud.google.com/compute/docs/access/service-accounts#accesscopesiam>`_ to Cloud Storage.
#. For each service account from the list, add `Storage Object Admin role <https://cloud.google.com/storage/docs/access-control/iam-roles>`_ in bucket permissions settings.

Add your Service Account Credentials the Scylla Manager Agent Configuration File
--------------------------------------------------------------------------------

This allows Scylla Manager to access the bucket. This procedure is done manually **on each** Scylla Node instance.
Alternatively you can configure service account credentials manually. Use `this instruction <https://cloud.google.com/docs/authentication/production#manually>`_ to get the service account file.


**Procedure**
#. Open the Scylla Manager Agent Configuration File for editing. It is located in ``/etc/scylla-manager-agent/scylla-manager-agent.yaml``.

   As this is a Yaml file, remember to indent two spaces for each line you uncomment.
   Refer to :ref:`Scylla Agent Configuration <configure-auth-token>` for details.

#. Uncomment the ``gcs:`` line.
#. Uncomment and set ``service_account_file`` with the path to the service account credentials file.
#. For each service account used by the nodes, add `Storage Object Admin role <https://cloud.google.com/storage/docs/access-control/iam-roles>`_ in the bucket permissions settings.
#. Validate that the manager has access to the backup location.
   If there is no response, the bucket is accessible. If not, you will see an error.

   .. code-block:: none

      $ scylla-manager-agent check-location --location gcs:<your GCS bucket name>

Troubleshoot Node to Bucket Connectivity
----------------------------------------

To troubleshoot Node to bucket connectivity issues you can run:

.. code-block:: none

   scylla-manager-agent check-location --debug --location gcs:<your GCS bucket name>
