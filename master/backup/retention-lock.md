# Retention Lock

ScyllaDB Manager can protect snapshot files from accidental or malicious deletion
by applying object-level retention locks on snapshot files stored in backup bucket.
When retention lock is enabled, snapshot files in the backup location cannot be
deleted until the retention period expires.

Retention lock is currently supported for [Google Cloud Storage](https://manager.docs.scylladb.com/master/backup/setup-gcs.md) only.

> * [How It Works](#how-it-works)
> * [Modes](#modes)
> * [Override Lock](#override-lock)
> * [Prerequisites](#prerequisites)
> * [Usage](#usage)
>   * [Creating retention lock configuration](#creating-retention-lock-configuration)
>   * [Changing retention lock configuration](#changing-retention-lock-configuration)
> * [Shared files](#shared-files)

## How It Works

When retention lock is enabled on a backup task, ScyllaDB Manager applies object-level retention
to all snapshot files from given backup task execution. This includes schema files, SSTable files,
and manifest files. The retention lock is applied during dedicated stage (`RETENTION_LOCK`) that
runs after backup is finalized and all snapshot files are already in backup location.

The retention period for each snapshot is calculated from the **snapshot creation timestamp**
contained in the snapshot tag, and lasts for the specified retention days.

This means that a snapshot with `--retention-days 30` will have its files protected for exactly 30 days
from when the snapshot was taken, regardless of how long the backup task takes to complete.

## Modes

Retention lock supports three modes controlled by the [sctool backup –retention-lock-mode](https://manager.docs.scylladb.com/master/sctool/backup.md#sctool-backup) flag:

* `disabled` (default): No retention lock is applied to snapshot files.
* `unlocked`: Retention lock is applied but can be shortened or removed with special permissions (see [Prerequisites]()).
* `locked`: Retention lock is applied and cannot be overridden. Once set, the lock cannot be removed or shortened, even by the bucket owner.

## Override Lock

The [sctool backup –override-retention-lock](https://manager.docs.scylladb.com/master/sctool/backup.md#sctool-backup) flag allows overriding previously
applied retention locks in `unlocked` mode. This flag is recommended in the following cases:

* **Changing mode from** `unlocked` **to** `locked`: When a previous backup was created with `unlocked` mode,
  upgrading to `locked` mode requires overriding the existing `unlocked` locks on shared files
  (see [Shared files]()) that are referenced by both the old and new snapshot.
* **Decreasing retention period in** `unlocked` **mode**: Shortening `--retention-days` requires overriding
  the previously set, longer retention period on shared files.

## Prerequisites

Google Cloud Storage

### Bucket configuration

The GCS bucket used as the backup location must have **Object Retention** enabled.
Refer to the [Enable and use object retention configurations documentation](https://docs.cloud.google.com/storage/docs/using-object-lock)
for instructions on creating a bucket with Object Retention enabled.

### Permissions

The GCS service account used by ScyllaDB Manager Agent must have the following permissions
on the backup bucket:

* `storage.objects.update` — required for updating object metadata.
* `storage.objects.setRetention` — required for applying retention locks to snapshot files.
* `storage.objects.overrideUnlockedRetention` — required when using the `--override-retention-lock` flag
  to modify or remove existing locks in `unlocked` mode.

These permissions are included in the following predefined IAM role:

* [Storage Object Admin](https://cloud.google.com/storage/docs/access-control/iam-roles) (`roles/storage.objectAdmin`)

These permissions are in addition to the standard permissions required for backup operations
as described in [Setup Google Cloud Storage](https://manager.docs.scylladb.com/master/backup/setup-gcs.md).

## Usage

### Creating retention lock configuration

You can [create a new backup task](https://manager.docs.scylladb.com/master/sctool/backup.md#sctool-backup) with retention lock enabled:

```none
sctool backup -c <cluster ID> -L gcs:<bucket> --retention-lock-mode locked --retention-days 30
```

You can also [update an existing backup task](https://manager.docs.scylladb.com/master/sctool/backup.md#backup-update) to enable retention lock:

```none
sctool backup update -c <cluster ID> <backup task ID> --retention-lock-mode unlocked --retention-days 14
```

Note that when retention lock is enabled:

* `--retention-days` should be set to a positive value.
* Count-based `--retention` should not be set.

### Changing retention lock configuration

The following guidelines are recommended when updating the retention lock mode or retention period
on an existing backup task or when configuring retention lock on multiple backup tasks executed
on the same DCs and tables. These constraints are not enforced by server-side validation, but
deviating from them may lead to errors during the purge stage (see [Shared files]()):

* Avoid changing the mode from `locked` to `unlocked`.
* When changing from `unlocked` to `locked`, use the `--override-retention-lock` flag (see [Override Lock]()).
* In `locked` mode, `--retention-days` should only be **increased**, not decreased.
* Decreasing `--retention-days` in `unlocked` mode should be accompanied by the `--override-retention-lock` flag (see [Override Lock]()).

## Shared files

ScyllaDB Manager deduplicates SSTables across backups — multiple snapshots (possibly from different backup tasks)
can reference the same underlying files. When retention lock is applied to a snapshot, it is also applied
to all files referenced by that snapshot, including files shared with other snapshots.

#### WARNING
Avoid configuring multiple backup tasks with different retention lock settings that target
the same subset of DCs or tables.

Conflicting retention lock configurations (e.g., different modes or retention periods) across
backup tasks that share overlapping files can cause errors during the purge stage.

At the end of each backup task execution, ScyllaDB Manager purges stale snapshots across all
registered backup tasks. If any backup task has a retention lock misconfiguration — for example,
a lower `--retention-days` value that conflicts with an existing backup created with a higher
`--retention-days` and `--retention-lock-mode=locked` — the purge stage will fail for all
backup tasks, not just the misconfigured one. Because the purge stage runs after the snapshot
upload is complete, the backup data itself is preserved. However, stale snapshots will accumulate,
increasing storage consumption and costs.

To resolve such errors, update the `--retention-days` and `--retention-lock-mode` flags
(along with `--override-retention-lock` if necessary) on the problematic backup task
to use values that are consistent with those of other tasks sharing the same files.
