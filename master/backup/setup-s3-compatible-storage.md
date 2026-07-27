# Setup S3 compatible storage

> * [Create a bucket](#create-a-bucket)
> * [Grant access](#grant-access)
>   * [Policy](#policy)
>   * [Config file](#config-file)
> * [Troubleshoot connectivity](#troubleshoot-connectivity)
> * [Custom root Certificate Authority (CA)](#custom-root-certificate-authority-ca)

There are multiple S3 API compatible providers that can be used with ScyllaDB Manager.
Due to minor differences between them we require that exact provider is specified in the config file for full compatibility.
The available providers are Alibaba, AWS, Ceph, DigitalOcean, IBMCOS, Minio, Wasabi, Dreamhost, Netease.

## Create a bucket

You need to create a bucket in your storage system of choice.

## Grant access

This procedure is required so that ScyllaDB Manager can access your bucket.
You need to configure bucket access policy in your storage system and set credentials in the ScyllaDB Manager Agent config file.

### Policy

The user must have list, read, write and delete privileges to the bucket, for specifics consult [the sample AWS IAM policy](https://manager.docs.scylladb.com/master/backup/setup-amazon-s3.md#aws-iam-policy).

#### MinIO Example

With MinIO you can use the the same policy files as with AWS.

**Procedure**

Given myminio is an alias for your MinIO deployment.

1. Create a user by providing access and secret keys.
   ```none
   mc admin user add myminio "${MINIO_USER_ACCESS_KEY}" "${MINIO_USER_SECRET_KEY}"
   ```
2. Copy [the sample AWS IAM policy](https://manager.docs.scylladb.com/master/backup/setup-amazon-s3.md#aws-iam-policy) as `user-policy.json`.
3. Replace `scylla-manager-backup` in `user-policy.json` with your bucket name.
4. Create a user policy.
   ```none
   mc admin policy add myminio sm-user-policy user-policy.json
   ```
5. Attach the policy to the user.
   ```none
   mc admin policy set myminio sm-user-policy user="${MINIO_USER_ACCESS_KEY}"
   ```

### Config file

Note that this procedure needs to be repeated for each ScyllaDB node.

**Procedure**

Edit the `/etc/scylla-manager-agent/scylla-manager-agent.yaml`

1. Uncomment the `s3:` line, for parameters note the two spaces in front, it’s a yaml file.
2. Uncomment and set `access_key_id` and `secret_access_key`.
3. Uncomment and set `provider` to one of the supported providers.
4. Uncomment and set `endpoint` to the base URL of your storage.
5. Validate that the manager has access to the backup location.
   If there is no response, the S3 bucket is accessible. If not, you will see an error.
   ```none
   scylla-manager-agent check-location --location s3:<your S3 bucket name>
   ```
6. Restart ScyllaDB Manager Agent service.
   ```none
   sudo systemctl restart scylla-manager-agent
   ```

#### MinIO Example

```yaml
s3:
  access_key_id: AKIAIOSFODNN7EXAMPLE
  secret_access_key: wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
  provider: Minio
  endpoint: http://192.168.121.99:9000
```

## Troubleshoot connectivity

To troubleshoot Node to bucket connectivity issues you can run:

```none
scylla-manager-agent check-location --debug --location s3:<your S3 bucket name>
```

## Custom root Certificate Authority (CA)

Some providers, such as MinIO, support the option to choose a custom Certificate Authority for verifying the authenticity.
See [Network Encryption (TLS)](https://min.io/docs/minio/linux/operations/network-encryption.html) in the MinIO documentation for details.

To ensure that ScyllaDB Manager functions correctly with MinIO’s enhanced security, you must add that specific Certificate Authority
to the system’s Certificate Authorities on every VM where the ScyllaDB Manager Agent is running.

> Example for Ubuntu

> ```console
> sudo cp <source_path>/rootCA.pem /usr/local/share/ca-certificates/rootCA.crt
> sudo update-ca-certificates
> ```

> Example for Centos

> ```console
> cp <source_path>/rootCA.pem /etc/pki/ca-trust/source/anchors/rootCA.crt
> update-ca-trust extract
> ```
