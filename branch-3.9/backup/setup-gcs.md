# Setup Google Cloud Storage

> * [Create a bucket](#create-a-bucket)
> * [Grant access](#grant-access)
>   * [Automatic service account authorization](#automatic-service-account-authorization)
>   * [Service account file](#service-account-file)
> * [Additional features](#additional-features)
> * [Troubleshoot connectivity](#troubleshoot-connectivity)

## Create a bucket

Go to [Google Cloud Storage](https://cloud.google.com/storage) and create a new bucket in a region where ScyllaDB nodes are.
If your cluster is deployed in multiple regions create a bucket per region.
You may decide to backup only a single datacenter to save on costs, in that case create only one bucket in a region you want to backup.

## Grant access

This procedure is required so that ScyllaDB Manager can access your bucket.

Choose how you want to configure access to the bucket.
If your application runs inside a Google Cloud environment we recommend using automatic Service account authentication.
Otherwise you can add your credentials to the agent configuration file.
The later method is less secure as you will be propagating each node with this security information and in cases where you need to change the key, you will have to replace it on each node.

### Automatic service account authorization

**Procedure**

1. Collect list of [service accounts](https://cloud.google.com/compute/docs/access/service-accounts) used by **each** of the ScyllaDB nodes.
2. Make sure that each of service account has read/write [access scope](https://cloud.google.com/compute/docs/access/service-accounts#accesscopesiam) to Cloud Storage.
3. For each service account from the list, add [Storage Object Admin role](https://cloud.google.com/storage/docs/access-control/iam-roles) in bucket permissions settings.

### Service account file

Note that this procedure needs to be repeated for each ScyllaDB node.

**Prerequisites**

Use [this instruction](https://cloud.google.com/docs/authentication/production#manually) to get the service account file.

**Procedure**

1. Upload service account file to `/etc/scylla-manager-agent/gcs-service-account.json`.
   If you want to use different path change service_account_file parameter in `gcs` section in [ScyllaDB Manager Agent Config file](https://manager.docs.scylladb.com/branch-3.9/config/scylla-manager-agent-config.md).
2. Validate that the manager has access to the backup location.
   If there is no response, the bucket is accessible. If not, you will see an error.
   ```none
   scylla-manager-agent check-location --location gcs:<your GCS bucket name>
   ```

## Additional features

You can enable additional Google Cloud Storage features such as **storage class** or **endpoint**.
Those need to be enabled on per Agent basis in the configuration file.
Check out the `gcs` section in [ScyllaDB Manager Agent Config file](https://manager.docs.scylladb.com/branch-3.9/config/scylla-manager-agent-config.md).

## Troubleshoot connectivity

To troubleshoot Node to bucket connectivity issues you can run:

```none
scylla-manager-agent check-location --debug --location gcs:<your GCS bucket name>
```
