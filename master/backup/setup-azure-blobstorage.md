# Setup Azure Blob Storage

> * [Create a container](#create-a-container)
> * [Grant access](#grant-access)
>   * [IAM role](#iam-role)
>   * [Config file](#config-file)
> * [Troubleshoot connectivity](#troubleshoot-connectivity)

To use Azure Blob Storage as your backup location you need a storage account, a container, and authentication setup.

## Create a container

Go to [Azure Portal](https://portal.azure.com/) and create a new container within your storage account.
This container should be only used for storing ScyllaDB Manager backups.
If your cluster is deployed in multiple regions create a storage account and container per region.
You may decide to backup only a single datacenter to save on costs, in that case create only one storage account and container in a region you want to backup.

## Grant access

This procedure is required so that ScyllaDB Manager can access your containers.

Choose how you want to configure access to the container.
You can use an [IAM role](#azure-iam-role) (recommended) or you can add storage account credentials (account/key) to the ScyllaDB Manager Agent configuration file.
The latter method is not recommended because you are placing the security information directly on each node, which is much less secure than the IAM role method. In addition, if you need to change the key, you will have to replace it on every node.

<a id="azure-iam-role"></a>

### IAM role

**Portal Procedure**

Procedure for configuring IAM role access over web UI:

1. Create custom [IAM role](https://docs.microsoft.com/en-us/azure/role-based-access-control/custom-roles-portal#start-from-json) for the storage account which adheres to your company security policy.
2. [Assign the custom IAM role](https://docs.microsoft.com/en-us/azure/role-based-access-control/role-assignments-portal) to **each virtual machine instance (node)** in the cluster.

<a id="azure-iam-policy"></a>

Sample role JSON definition scoped to the *ScyllaManagerBackup* resource group:

```json
{
  "properties": {
    "roleName": "Scylla Backup Storage Contributor",
    "description": "Contributor access to the blob service for Scylla cluster backups",
    "assignableScopes": [
      "/subscriptions/<subscription_uuid>/resourceGroups/ScyllaManagerBackup"
    ],
    "permissions": [
      {
        "actions": [
          "Microsoft.Storage/storageAccounts/blobServices/containers/delete",
          "Microsoft.Storage/storageAccounts/blobServices/containers/read",
          "Microsoft.Storage/storageAccounts/blobServices/containers/write",
          "Microsoft.Storage/storageAccounts/blobServices/generateUserDelegationKey/action"
        ],
        "notActions": [],
        "dataActions": [
          "Microsoft.Storage/storageAccounts/blobServices/containers/blobs/delete",
          "Microsoft.Storage/storageAccounts/blobServices/containers/blobs/read",
          "Microsoft.Storage/storageAccounts/blobServices/containers/blobs/write",
          "Microsoft.Storage/storageAccounts/blobServices/containers/blobs/move/action",
          "Microsoft.Storage/storageAccounts/blobServices/containers/blobs/add/action"
        ],
        "notDataActions": []
      }
    ]
  }
}
```

You can use permissions from the provided sample but make sure to set proper value for `assignableScopes` field because that is specific to your environment.

### Config file

Note that this procedure needs to be repeated for each ScyllaDB node.

**Procedure**

Edit the `/etc/scylla-manager-agent/scylla-manager-agent.yaml`

1. Uncomment the `azure:` line, for parameters note the two spaces in front, it’s a yaml file.
2. Uncomment and set `account:` line under `azure:`. This field must always be set to the name of the storage account that backup container lives under.
3. Optionally uncomment and set `key:` line under `azure:` if you are using account/key based access. If you are using role based access keep this line commented.
4. Validate that the manager has access to the backup location.
   If there is no response, the container is accessible. If not, you will see an error.
   ```none
   scylla-manager-agent check-location --location azure:<blob storage container name>
   ```
5. Restart ScyllaDB Manager Agent service.
   ```none
   sudo systemctl restart scylla-manager-agent
   ```

## Troubleshoot connectivity

To troubleshoot ScyllaDB node to bucket connectivity issues you can run:

```none
scylla-manager-agent check-location --debug --location azure:<blob storage container name>
```
