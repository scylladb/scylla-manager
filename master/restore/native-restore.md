# Restore Methods

ScyllaDB Manager restores the content of the tables with one of the following restore methods:

* `rclone`: the default method, in which ScyllaDB Manager Agent downloads SSTables from the backup location and ScyllaDB streams them into the cluster
* `native`: [native restore](), in which ScyllaDB streams SSTables into the cluster directly from the backup location
* `tablet`: [tablet aware restore](), in which ScyllaDB restores tables replicated with [tablets](https://docs.scylladb.com/manual/stable/architecture/tablets.html) directly from the backup location

ScyllaDB Manager uses ScyllaDB Manager Agents deployed on each ScyllaDB node to coordinate restore.
These agents serve as a proxy to [ScyllaDB REST API](https://docs.scylladb.com/manual/stable/operating-scylla/rest.html) and also act as [rclone](https://github.com/scylladb/rclone) servers responsible for communication between the node and backup location.

Since rclone server is separate from ScyllaDB internal schedulers, yet they both live on the same machine,
it is resource-limited at the cgroup level (agent runs in `scylla-helper.slice` under `scylla.slice` with lower CPU/IO priority).
Moreover, having separate processes responsible for fetching files from backup location and streaming those files into the cluster results
in the need of additional costly synchronization.

This solution results in:

* Saving files downloaded from backup location on disk before streaming them into the cluster (unnecessary disk writes before streaming)
* Inefficient resource utilization (file download handled by rclone server while file streaming handled by ScyllaDB)
* Longer restore duration (limited resources allocated to rclone server)

Native and tablet aware restore aim to solve these problems by moving restore responsibilities from rclone server into ScyllaDB itself,
similarly to [native backup](https://manager.docs.scylladb.com/master/backup/native-backup.md).
Just like with native backup, all restore methods rely on the same [backup specification](https://manager.docs.scylladb.com/master/backup/specification.md),
so any backup (native or rclone) can be restored using any restore method (assuming it meets the given restore method requirements).

## Comparison and limitations

The `ScyllaDB version` row describes the ScyllaDB version from which the method is considered production ready,
even though the method might be available in earlier versions as well.

|                                    | `rclone`                                                                                                                                   | `native`                                                                                                                                                   | `tablet`                                                                                                                                                   |
|------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------|
| ScyllaDB version                   | any                                                                                                                                        | 2026.1                                                                                                                                                     | 2026.4                                                                                                                                                     |
| Backup location providers          | all                                                                                                                                        | [s3](https://manager.docs.scylladb.com/master/backup/setup-s3-compatible-storage.md), [gcs](https://manager.docs.scylladb.com/master/backup/setup-gcs.md)  | [s3](https://manager.docs.scylladb.com/master/backup/setup-s3-compatible-storage.md), [gcs](https://manager.docs.scylladb.com/master/backup/setup-gcs.md)  |
| ScyllaDB configuration             | none                                                                                                                                       | `object_storage_endpoints` (see [Configuration]())                                                                                                         | `object_storage_endpoints` (see [Configuration]())                                                                                                         |
| Restored tables                    | all                                                                                                                                        | all                                                                                                                                                        | replicated with tablets in both backed up and restore destination cluster                                                                                  |
| Backup requirements                | none                                                                                                                                       | no SSTables with integer based IDs                                                                                                                         | created with ScyllaDB Manager 3.9 or later from ScyllaDB 2026.1.1 or later, no SSTables with integer based IDs                                             |
| Cluster topology                   | any                                                                                                                                        | any                                                                                                                                                        | restore target and backed up clusters are single data center only, they have the same data center and corresponding rack names                             |
| Fallback to less preferred methods | not needed                                                                                                                                 | per batch                                                                                                                                                  | per table                                                                                                                                                  |
| Restore speed control              | [–batch-size –parallel –transfers –rate-limit –unpin-agent-cpu](https://manager.docs.scylladb.com/master/sctool/restore.md#sctool-restore) | [stream_io_throughput_mb_per_sec](https://docs.scylladb.com/manual/stable/reference/configuration-parameters.html#confval-stream_io_throughput_mb_per_sec) | [stream_io_throughput_mb_per_sec](https://docs.scylladb.com/manual/stable/reference/configuration-parameters.html#confval-stream_io_throughput_mb_per_sec) |
| `scylla_manager_restore_*` metrics | `remaining_bytes`, `progress`, `batch_size`, `state`, `downloaded_bytes`, `download_duration`, `streamed_bytes`, `stream_duration`         | `remaining_bytes`, `progress`, `batch_size`, `state`, `restored_bytes`, `restore_duration`                                                                 | `remaining_bytes`, `progress`                                                                                                                              |

## Native restore

Native restore moves the download and streaming of SSTables from the rclone server to ScyllaDB.
ScyllaDB node decomposes SSTable obtained from backup location into individual mutations and streams them to node
being their primary replica. It leverages ScyllaDB’s efficient communication with backup location and allows for
not saving downloaded SSTables on disk before streaming them into the cluster. Even though ScyllaDB does the most
time consuming part, all other stages of the restore procedure (e.g., repair, view rebuild, etc.) are still
orchestrated by ScyllaDB Manager.

Native restore works on per restored batch basis, so when combined with the `rclone` method,
it is used for most restored batches, even if a small subset of them contains SSTables not compatible with native restore.

<a id="tablet-aware-restore"></a>

## Tablet aware restore

Tablet aware restore is a further improvement over the native restore.
Instead of reading mutations on one node and streaming them to another,
in tablet aware restore entire SSTables are downloaded right to the node
which owns them, resulting in an even bigger performance gain. Similar
to native restore, all other stages of restore procedure are still orchestrated
by ScyllaDB Manager.

Tablet aware restore works on per restored table basis, so when combined with other methods,
tables not meeting its requirements (see [Comparison and limitations]()) are restored with the next allowed method.
Note that [sctool restore –dc-mapping](https://manager.docs.scylladb.com/master/sctool/restore.md#sctool-restore) can be used to exclude data centers from the restore, but not to rename them.

Pausing the restore task aborts the ScyllaDB restore tasks of the tables that are being restored.
Resuming the restore task restores them again, skipping only the tables that were already fully restored.

## Configuration

Native and tablet aware restore require the same ScyllaDB and ScyllaDB Manager Agent configuration as native backup.
Follow the steps described in [Configuration](https://manager.docs.scylladb.com/master/backup/native-backup.md#configure-native-backup-in-scylla) to configure each ScyllaDB node.
The [stream_io_throughput_mb_per_sec](https://docs.scylladb.com/manual/stable/reference/configuration-parameters.html#confval-stream_io_throughput_mb_per_sec)
shouldn’t be throttled when restoring into a cluster which doesn’t currently serve user traffic.

## Usage

The restore method is controlled with the [sctool restore –method](https://manager.docs.scylladb.com/master/sctool/restore.md#sctool-restore) flag.
It accepts a comma separated list of allowed methods (`tablet`, `native`, `rclone`) or the special value `auto`, which allows all of them.
The conservative default value is `rclone`, but it is recommended to use `auto` in most real scenarios.
Setting explicit methods could be useful for validation, testing, and various workarounds when encountering unexpected issues.

When more than one method is allowed, each restored table uses the most preferred allowed method that supports it.
Methods are preferred in the following order:

> * `tablet`: Uses [tablet aware restore]()  (best performance, tablets only, has additional limitations).
> * `native`: Uses [native restore]() (good performance, both tablets and vnodes, has small limitations).
> * `rclone`: Uses rclone restore (worst performance, supports all scenarios).
> * `auto`: Allows all methods and uses each of them only when it is considered production ready (see [Comparison and limitations]()).

Selected method validation:

> * Explicitly listed methods are required to be usable by the restore target cluster. Otherwise, creating the restore task
>   (also with `--dry-run`) or running it fails right away, even if another allowed method could be used instead.
>   This applies to the `ScyllaDB configuration` requirement and, in case of `tablet`, to the restore target cluster
>   consisting of a single data center and `--dc-mapping` not renaming data centers.
>   For example, `--method tablet,rclone` fails for a restore target cluster with multiple data centers.
>   On the other hand, the `ScyllaDB version` requirement is not validated for explicitly listed methods,
>   so they can be used, at your own risk, with ScyllaDB versions in which they are available, but not yet considered production ready.
> * `auto` skips the methods that are not usable by the restore target cluster or are not considered production ready
>   for its ScyllaDB version, and uses the remaining ones.

In both cases, all the other requirements depend on the backup and restored tables, so they are validated per restored table or batch,
which is restored with the next allowed method when it does not meet them.
The restore fails when some table can’t be restored with any of the allowed methods,
e.g., when using `--method tablet` for tables replicated with vnodes.

The methods used for restoring each table are displayed by [sctool progress](https://manager.docs.scylladb.com/master/sctool/progress.md#task-progress).

Note that [sctool restore –rate-limit –transfers –unpin-agent-cpu](https://manager.docs.scylladb.com/master/sctool/restore.md#sctool-restore) flags do not take effect when using native or tablet aware restore,
as the streaming performance is controlled directly by ScyllaDB itself. To control streaming performance on the ScyllaDB side,
configure [stream_io_throughput_mb_per_sec](https://docs.scylladb.com/manual/stable/reference/configuration-parameters.html#confval-stream_io_throughput_mb_per_sec) in scylla.yaml.

You can [create restore task](https://manager.docs.scylladb.com/master/sctool/restore.md#sctool-restore) with the desired methods:

```none
sctool restore -c <cluster ID> -L <backup location> --snapshot-tag <tag> --method native
sctool restore -c <cluster ID> -L <backup location> --snapshot-tag <tag> --method tablet,native
```
