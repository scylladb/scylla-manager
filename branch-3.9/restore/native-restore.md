# Native Restore

Native restore is an optimization similar to [native backup](https://manager.docs.scylladb.com/branch-3.9/backup/native-backup.md).

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

Native restore aims to solve these problems by moving restore responsibilities from rclone server into ScyllaDB itself.
Just like with native backup, both native and rclone restores performed by ScyllaDB Manager rely on the same [backup specification](https://manager.docs.scylladb.com/branch-3.9/backup/specification.md),
so any backup (native or rclone) can be restored using any restore method (assuming it meets given restore method limitations).
In the [Status]() section you can find the parts of restore procedure already moved to ScyllaDB.

## Status

This section contains the list of stages in the restore procedure now managed by ScyllaDB.
All other stages are still performed by the rclone server.
The `ScyllaDB Version` column describes the ScyllaDB version from which the functionality is considered production ready,
even though the functionality might be available in earlier versions as well.

| Functionality                                                                                                            |   ScyllaDB Version | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          | Limitations                                                                                                                                                                                                                |
|--------------------------------------------------------------------------------------------------------------------------|--------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| SSTable streaming directly from [s3](https://manager.docs.scylladb.com/branch-3.9/backup/setup-s3-compatible-storage.md) |             2026.1 | As this is the most time- and resource-consuming part of the restore procedure, moving it to ScyllaDB brings the most benefits.<br/>It also allows for not saving downloaded SSTables on disk before streaming them into the cluster.<br/>Unlike native backup, when performing restore on a cluster which doesn’t currently serve user traffic,<br/>it’s best not to throttle native restore with<br/>[stream_io_throughput_mb_per_sec](https://docs.scylladb.com/manual/stable/reference/configuration-parameters.html#confval-stream_io_throughput_mb_per_sec) in scylla.yaml<br/>to obtain the best performance. | Does not support restoration of [versioned SSTables](https://manager.docs.scylladb.com/branch-3.9/backup/specification.md#backup-versioned-sstables).<br/>Does not support restoration of SSTables with integer based IDs. |
| SSTable streaming directly from [gcs](https://manager.docs.scylladb.com/branch-3.9/backup/setup-gcs.md)                  |             2026.1 | Same as above.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       | Same as above.                                                                                                                                                                                                             |

## Configuration

Native restore requires the same ScyllaDB and ScyllaDB Manager Agent configuration as native backup.
Follow the steps described in [Configuration](https://manager.docs.scylladb.com/branch-3.9/backup/native-backup.md#configure-native-backup-in-scylla) to configure each ScyllaDB node.
Throttling [stream_io_throughput_mb_per_sec](https://docs.scylladb.com/manual/stable/reference/configuration-parameters.html#confval-stream_io_throughput_mb_per_sec)
is not recommended, as explained in [Status]() section.

## Usage

The native restore usage is controlled with the [sctool restore –method](https://manager.docs.scylladb.com/branch-3.9/sctool/restore.md#sctool-restore) flag.
It supports three values: `rclone` (default), `native`, and `auto`:

> * `native`: Uses all native restore functionalities listed in the [Status]() section.
>   Use this value for native restore configuration validation and testing.
>   Note that this will fail when:
>   * ScyllaDB is not configured properly (see `object_storage_endpoints` in [Configuration]())
>   * Provider not supported by used ScyllaDB version is used (see limitations in [Status]())
>   * Restored backup contains versioned SSTables (see limitations in [Status]())
>   * Restored backup contains SSTables with integer based IDs (see limitations in [Status]())
> * `auto`: Uses native restore functionalities when possible, otherwise falls back to rclone restore.
>   Use this value for production restores. It will use native restore functionality only when it is
>   considered production ready (see version support in [Status]()). The fallback works on per restored batch basis,
>   so it allows for utilizing native restore functionalities for most restored batches,
>   even if a small subset of them contains SSTables not compatible with native restore.
> * `rclone`: Uses rclone restore functionalities only. This effectively disables all native restore functionalities.

Note that [sctool restore –rate-limit –transfers –unpin-agent-cpu](https://manager.docs.scylladb.com/branch-3.9/sctool/restore.md#sctool-restore) flags do not take effect when using native restore,
as the streaming performance is controlled directly by ScyllaDB itself. To control streaming performance on the ScyllaDB side,
configure [stream_io_throughput_mb_per_sec](https://docs.scylladb.com/manual/stable/reference/configuration-parameters.html#confval-stream_io_throughput_mb_per_sec) in scylla.yaml.

You can [create restore task](https://manager.docs.scylladb.com/branch-3.9/sctool/restore.md#sctool-restore) with the desired method:

```none
sctool restore -c <cluster ID> -L <backup location> --snapshot-tag <tag> --method native
```
