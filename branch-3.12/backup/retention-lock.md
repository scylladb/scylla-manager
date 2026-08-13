# Retention Lock

ScyllaDB Manager can protect snapshot files from accidental or malicious deletion
by applying object retention locks or event based holds on snapshot files stored in backup bucket.
When retention lock is enabled, snapshot files in the backup location cannot be
deleted until the retention period expires.

Retention lock is currently supported for [Google Cloud Storage](https://manager.docs.scylladb.com/stable/backup/setup-gcs.md) only.

> * [How It Works](#how-it-works)
>   * [Object retention lock modes (`unlocked`, `locked`)](#object-retention-lock-modes-unlocked-locked)
>   * [Event based hold mode (`event-based-hold`)](#event-based-hold-mode-event-based-hold)
> * [Modes](#modes)
> * [Override Lock](#override-lock)
> * [Prerequisites](#prerequisites)
> * [Usage](#usage)
>   * [Creating retention lock configuration](#creating-retention-lock-configuration)
>   * [Changing retention lock configuration](#changing-retention-lock-configuration)
> * [Shared files](#shared-files)

## How It Works

### Object retention lock modes (`unlocked`, `locked`)

When object retention lock is enabled on a backup task, ScyllaDB Manager applies object-level retention
to all snapshot files from given backup task execution. This includes schema files, SSTable files,
and manifest files. The retention lock is applied during dedicated stage (`RETENTION_LOCK`) that
runs after backup is finalized and all snapshot files are already in backup location.

The retention period for each snapshot is calculated from the **snapshot creation timestamp**
contained in the snapshot tag, and lasts for the specified retention days.

This means that a snapshot with `--retention-days 30` will have its files protected for exactly 30 days
from when the snapshot was taken, regardless of how long the backup task takes to complete.

This approach results in making a per-file request for both newly uploaded and deduplicated files.
When making highly deduplicated backups to a colder storage tier (low storage costs, high request costs),
consider using `event-based-hold` mode which avoids making additional requests for deduplicated files.

### Event based hold mode (`event-based-hold`)

This mode utilizes the following cloud provider features for protecting snapshot files:

* [Default bucket event based holds](https://docs.cloud.google.com/storage/docs/object-holds#default-holds) - uploaded
  objects have event based hold set. This hold needs to be removed before protected object can be deleted.
* [Bucket retention lock](https://docs.cloud.google.com/storage/docs/bucket-lock) - removing event based hold from an
  object starts specified retention period during which the object can’t be deleted.

ScyllaDB Manager utilizes those bucket features in the following way:

* Newly uploaded snapshot files automatically have event based hold applied.
* Deduplicated snapshot files keep their hold.
* Files referenced by previous snapshot which are not a part of the current one have their event based holds released. This starts their retention period.

The retention period is configured via bucket configuration, not the `--retention-days` or `--retention` flags.
ScyllaDB Manager won’t attempt to purge stale snapshots according to backup task retention policy,
if they are still protected by either event based holds or already started retention period.

The main benefit of this approach is that it makes only a single per-file request and does not repeat those requests for deduplicated files.
When making highly deduplicated backups to a colder storage tier (low storage costs, high request costs),
it’s possible that the request costs can dominate the overall backup costs.
`event-based-hold` mode aims to reduce costs in such scenarios.

Note that since the holds for previous snapshot are released only during the backup task execution,
corresponding files won’t be removed from the backup storage for at least `bucket_retention_period + backup_task_interval`.

Note that because all objects uploaded to the bucket are subject to the default retention policy,
files coming from aborted backups, temporary manifests and permission check files can’t be removed
until their holds are released and the retention period expires.

## Modes

Retention lock supports the following modes controlled by the [sctool backup –retention-lock-mode](https://manager.docs.scylladb.com/stable/sctool/backup.md#sctool-backup) flag:

* `disabled` (default): No retention lock is applied to snapshot files.
* `unlocked`: Retention lock is applied but can be shortened or removed with special permissions (see [Prerequisites]()).
* `locked`: Retention lock is applied and cannot be overridden. Once set, the lock cannot be removed or shortened, even by the bucket owner.
* `event-based-hold`: While referenced by the newest snapshot, files are protected by default event based holds. After that, they are protected by default retention period.

## Override Lock

The [sctool backup –override-retention-lock](https://manager.docs.scylladb.com/stable/sctool/backup.md#sctool-backup) flag allows overriding previously
applied retention locks in `unlocked` mode. This flag is recommended in the following cases:

* **Changing mode from** `unlocked` **to** `locked`: When a previous backup was created with `unlocked` mode,
  upgrading to `locked` mode requires overriding the existing `unlocked` locks on shared files
  (see [Shared files]()) that are referenced by both the old and new snapshot.
* **Decreasing retention period in** `unlocked` **mode**: Shortening `--retention-days` requires overriding
  the previously set, longer retention period on shared files.

## Prerequisites

Google Cloud Storage

### Bucket configuration for `unlocked` and `locked` modes

The GCS bucket used as the backup location must have **Object Retention** enabled.
Refer to the [Enable and use object retention configurations documentation](https://docs.cloud.google.com/storage/docs/using-object-lock)
for instructions on creating a bucket with Object Retention enabled.

### Bucket configuration for `event-based-hold` mode

The GCS bucket used as the backup location must have a **default retention policy** configured -
it defines the protection period of snapshot files (see [How It Works]()).
Refer to the [Use and lock retention policies](https://docs.cloud.google.com/storage/docs/using-bucket-lock#set-policy)
for instructions on setting a default retention policy on a bucket.

It is also recommended to enable the **default event based hold** option on the bucket,
so that the initial request setting the hold can be avoided.
Refer to the [Use object holds](https://docs.cloud.google.com/storage/docs/holding-objects#set-default-hold) for details.

### Permissions

The GCS service account used by ScyllaDB Manager Agent must have the following permissions
on the backup bucket:

* `storage.objects.update` — required for updating object metadata.
* `storage.objects.setRetention` — required for applying retention locks to snapshot files in `unlocked` and `locked` modes.
* `storage.objects.overrideUnlockedRetention` — required when using the `--override-retention-lock` flag
  to modify or remove existing locks in `unlocked` mode.

These permissions are included in the following predefined IAM role:

* [Storage Object Admin](https://cloud.google.com/storage/docs/access-control/iam-roles) (`roles/storage.objectAdmin`)

These permissions are in addition to the standard permissions required for backup operations
as described in [Setup Google Cloud Storage](https://manager.docs.scylladb.com/stable/backup/setup-gcs.md).

## Usage

### Creating retention lock configuration

You can [create a new backup task](https://manager.docs.scylladb.com/stable/sctool/backup.md#sctool-backup) with retention lock enabled:

```none
sctool backup -c <cluster ID> -L gcs:<bucket> --retention-lock-mode locked --retention-days 30
```

You can also [update an existing backup task](https://manager.docs.scylladb.com/stable/sctool/backup.md#backup-update) to enable retention lock:

```none
sctool backup update -c <cluster ID> <backup task ID> --retention-lock-mode unlocked --retention-days 14
```

Note that when `unlocked` or `locked` retention lock mode is enabled:

* `--retention-days` should be set to a positive value.
* Count-based `--retention` should not be set.

The `event-based-hold` mode can be combined with any retention policy, but the most intuitive
configuration is to set `--retention-days` to the same value as the bucket’s retention period:

```none
sctool backup -c <cluster ID> -L gcs:<bucket> --retention-lock-mode event-based-hold --retention-days <bucket retention period days>
```

### Changing retention lock configuration

The following guidelines are recommended when updating the retention lock mode or retention period
on an existing backup task or when configuring retention lock on multiple backup tasks executed
on the same DCs and tables. These constraints are not enforced by server-side validation, but
deviating from them may lead to errors during the purge stage (see [Shared files]()):

* Avoid changing the mode from `locked` to `unlocked`.
* When changing from `unlocked` to `locked`, use the `--override-retention-lock` flag (see [Override Lock]()).
* In `locked` mode, `--retention-days` should only be **increased**, not decreased.
* Decreasing `--retention-days` in `unlocked` mode should be accompanied by the `--override-retention-lock` flag (see [Override Lock]()).

#### WARNING
Changing the retention lock mode between `unlocked`/`locked` and `event-based-hold`
(in either direction) is **not supported**. These approaches to protecting snapshot files
are not compatible with each other and require different bucket configurations.
To switch between them, create a new backup task pointing to a different backup location
configured for the desired mode.

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
