# Backup

> * [Features](#features)
> * [Selecting tables and nodes to back up](#selecting-tables-and-nodes-to-back-up)
> * [Process](#process)
> * [Backup location](#backup-location)
> * [Removing backups](#removing-backups)

Using [sctool backup](https://manager.docs.scylladb.com/branch-3.9/sctool/backup.md#sctool-backup) command, you can schedule a backup of a managed cluster.
Backups and repairs are scheduled in the same manner, you can start, stop, resume, and track task progress on demand.

#### NOTE
If you are using ScyllaDB Manager deployed by ScyllaDB Operator, see dedicated [ScyllaDB Operator documentation](https://operator.docs.scylladb.com/stable/architecture/manager.html).

The following backup storage engines are supported:

* Amazon S3,
* S3 compatible API storage providers such as Ceph or MinIO,
* Google Cloud Storage.

For the purposes of backing up schema, ScyllaDB Manager requires CQL credentials ([sctool cluster update –username –password](cluster-update)).
In case of an Alternator cluster, ScyllaDB Manager additionally requires Alternator credentials ([sctool cluster update –alternator-access-key-id –alternator-secret-access-key](cluster-update)).
Alternator credentials can point to the same underlying CQL role as the CQL credentials (See [Alternator docs](https://docs.scylladb.com/manual/stable/alternator/compatibility.html#authentication-and-authorization) for details).

## Features

* Glob patterns to select keyspaces or tables to back up
* Deduplication of SSTables
* Retention of old data
* Throttling of upload speed
* Configurable upload destination per datacenter
* Pause and resume

## Selecting tables and nodes to back up

The `--keyspace`/`--dc` flags allow for specifying glob pattern for selecting tables/data centers to back up.
<br/>
Even when table should be backed up according to `--keyspace` flag, but it is not replicated in specified data centers (`--dc` flag), the table won’t be backed up.
<br/>
All currently down nodes are ignored for the backup procedure.
<br/>
In case table should be backed up, but some of its token ranges are not replicated on any currently live node in the cluster, the backup will fail.
<br/>
[Materialized Views](https://docs.scylladb.com/manual/stable/features/materialized-views.html) and [Secondary Indexes](https://docs.scylladb.com/manual/stable/features/secondary-indexes.html)
won’t be backed up, as they should be restored by recreating them on the restored base table (see [ScyllaDB docs](https://docs.scylladb.com/manual/stable/operating-scylla/procedures/backup-restore/restore.html#repeat-the-following-steps-for-each-node-in-the-cluster)).
<br/>
In order to ensure that data residing in View table is preserved, make sure to backup its base table.
<br/>
[LWT state tables](https://docs.scylladb.com/manual/stable/features/lwt.html#paxos-state-tables) won’t be backed up,
as they only store the state of ongoing LWT queries and do not store user data. Restoring these tables is not supported either.
<br/>

## Process

The backup procedure consists of multiple steps executed sequentially.

1. **Snapshot** - Take a snapshot of data on each node (according to backup configuration settings).

   Note that ScyllaDB Manager halts [tablets](https://docs.scylladb.com/manual/stable/architecture/tablets.html)  migration for the duration of this step.
2. **Schema** - Upload the schema in CQL text format to the backup storage destination,
   this requires that you added the cluster with CQL username and password.
   If you didn’t you can [update the cluster using sctool](https://manager.docs.scylladb.com/branch-3.9/sctool/cluster.md#cluster-update) at any point in time.

   Starting from ScyllaDB 6.0 and 2024.2 (and compatible ScyllaDB Manager 3.3),
   these CQL files are necessary for schema restoration ([sctool restore –restore-schema](https://manager.docs.scylladb.com/branch-3.9/sctool/restore.md#sctool-restore)).
3. **Upload** - Upload the snapshot to the backup storage destination.
4. **Manifest** - Upload the manifest file containing metadata about the backup.
5. **Purge** - If the retention threshold has been reached, remove the oldest backup from the storage location.

<a id="backup-location"></a>

## Backup location

You need to create a backup location for example an S3 bucket.
We recommend creating it in the same region as ScyllaDB nodes to minimize cross region data transfer costs.
In multi-dc deployments you should create a bucket per datacenter, each located in the datacenter’s region.

Details may differ depending on the storage engine, please consult:

* [Setup Amazon S3](https://manager.docs.scylladb.com/branch-3.9/backup/setup-amazon-s3.md)
* [Setup S3 compatible storage](https://manager.docs.scylladb.com/branch-3.9/backup/setup-s3-compatible-storage.md)
* [Setup Google Cloud Storage](https://manager.docs.scylladb.com/branch-3.9/backup/setup-gcs.md)
* [Setup Azure Blob Storage](https://manager.docs.scylladb.com/branch-3.9/backup/setup-azure-blobstorage.md)

## Removing backups

Backups may require a lot of storage space. They are purged according to the retention defined on the backup task.

Sctool can be used to remove snapshots of clusters that are no longer managed by ScyllaDB Manager.
The removal process is performed through the ScyllaDB Manager Agent installed on ScyllaDB nodes.

However, it’s recommended to delete the snapshots from the storage before removing the cluster from ScyllaDB Manager.
Otherwise, you will need to add the cluster again, list the snapshots in the given location, and remove them using the new cluster as the coordinator.
Another option is to purge them manually. If you want to remove the snapshots manually, please refer to the [backup specification](https://manager.docs.scylladb.com/branch-3.9/backup/specification.md)
and remove them accordingly.

Interrupted ScyllaDB Manager backup tasks might leave not yet uploaded snapshots on nodes’ disks.
They are automatically cleaned up after backup task finishes its execution (either after backup was resumed or it was started from scratch).
In case those snapshots are not needed and only result in disk space amplification, they can be cleaned up manually with
[sctool backup delete local-snapshots](https://manager.docs.scylladb.com/branch-3.9/sctool/backup.md#backup-delete-local-snapshots) command.
