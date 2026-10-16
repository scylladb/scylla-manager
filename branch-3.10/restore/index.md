# Restore

> * [Restore types](#restore-types)
> * [Features](#features)
> * [Restore speed observability](#restore-speed-observability)
> * [Restore speed control](#restore-speed-control)
> * [Interference with backup and repair tasks](#interference-with-backup-and-repair-tasks)

The [sctool restore](https://manager.docs.scylladb.com/branch-3.10/sctool/restore.md#sctool-restore) command allows you to run a restore of backed-up data (identified by its snapshot-tag) into a cluster.
Restore and backups are scheduled in the same manner: you can start, stop, resume, and track task progress on demand.

#### NOTE
If you are using ScyllaDB Manager deployed by ScyllaDB Operator, see dedicated [ScyllaDB Operator documentation](https://operator.docs.scylladb.com/stable/resources/scyllaclusters/nodeoperations/restore.html).

#### WARNING
Restore works only when ScyllaDB version of backed-up cluster is the same or older than ScyllaDB version of restore destination cluster.

## Restore types

Restore task has to be one of two types:

> * [restore tables](https://manager.docs.scylladb.com/branch-3.10/restore/restore-tables.md) - restores the content of the tables (rows)
>   * [1-1 restore](https://manager.docs.scylladb.com/branch-3.10/restore/restore-tables.md#restore) - faster restore of tables, please check the limitations before using it
> * [restore schema](https://manager.docs.scylladb.com/branch-3.10/restore/restore-schema.md) - restores the ScyllaDB cluster schema

If both the schema and the content of the tables need to be restored, you must start with restoring the schema. Only after the schema is successfully restored can you proceed with restoring the content of the tables.

## Features

ScyllaDB Manager Restore command supports the following features:

* Glob patterns to select keyspaces or tables to restore
* Control over the [restore speed and granularity](#restore-speed-and-granularity)
* [Dry run](https://manager.docs.scylladb.com/branch-3.10/restore/examples.md#restore-dry-run) - Test restore before live execution
* Progress tracking ([sctool progress](https://manager.docs.scylladb.com/branch-3.10/sctool/progress.md#task-progress), Prometheus metrics, [ScyllaDB Monitoring](https://monitoring.docs.scylladb.com) Manager dashboard)
* [Pausing](https://manager.docs.scylladb.com/branch-3.10/sctool/stop.md#task-stop) and [resuming](https://manager.docs.scylladb.com/branch-3.10/sctool/start.md#task-start) at any point of the process

## Restore speed observability

Restore speed can be checked with [sctool progress](https://manager.docs.scylladb.com/branch-3.10/sctool/progress.md#task-progress) command.
<br/>
It displays average per shard download and load&stream bandwidths. When used with `--details` flag, it also displays per host bandwidths.
<br/>

Restore speed can be also observed with Prometheus metrics:

* `scylla_manager_restore_remaining_bytes`
* `scylla_manager_restore_downloaded_bytes`
* `scylla_manager_restore_download_duration`
* `scylla_manager_restore_streamed_bytes`
* `scylla_manager_restore_stream_duration`

## Restore speed control

<a id="restore-speed-and-granularity"></a>

Restore speed is controlled by many parameters (see [sctool restore](https://manager.docs.scylladb.com/branch-3.10/sctool/restore.md#sctool-restore) documentation for details):

* `--batch-size`
* `--parallel`
* `--transfers`
* `--rate-limit`
* `--unpin-agent-cpu`
* `--allow-compaction`

Most of those parameters have default values chosen for restoring as fast as possible.
<br/>
You should need to change them only when you want to limit the impact that the restore has on a cluster serving traffic on not currently restored tables.
<br/>
For backward compatibility reasons, the default value of `--batch-size` is `2`, but it should be changed to `0` when you want to maximize restore speed.
<br/>
Note that with bigger batch size comes lesser granularity. This means that pausing and resuming restore would need to perform more work.
<br/>

The `--unpin-agent-cpu` is disabled by default, but in case you observe small download
bandwidth, you could try to [pause](https://manager.docs.scylladb.com/branch-3.10/sctool/stop.md#task-stop) restore task, [update](https://manager.docs.scylladb.com/branch-3.10/sctool/restore.md#restore-update) it with `--unpin-agent-cpu`,
and [resume](https://manager.docs.scylladb.com/branch-3.10/sctool/start.md#task-start) it.

## Interference with backup and repair tasks

The restore task will not start if backup or repair tasks are running in the cluster at that time, and vice versa, if a restore task is running, backup or repair tasks will not start.
You will see an error in the output of the `sctool tasks` command if there is a conflict between tasks.
Here is an example output of `sctool progress -c ${cluster_name} {task_id}` when there is an interference between tasks:

```default
$  ./sctool.dev progress -c my-cluster backup/06cd5d90-6daa-4215-acd3-d19f6782b5b6
Run:            351378ee-ab05-11ef-aa2d-0242c0a8c802
Status:         ERROR (initialising)
Cause:          exclusive task (restore) is running: another task is running
Start time:     25 Nov 24 09:13:52 CET
End time:       25 Nov 24 09:13:52 CET
Duration:       0s
Progress:       -
```
