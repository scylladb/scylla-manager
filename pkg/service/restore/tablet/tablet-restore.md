```markdown
# Tablet-aware restore (MS1)

Tablet-aware restore is a project to implement faster restore procedure for tablet tables only.  
In the initial milestones, backup/restore procedures will still be orchestrated by Scylla Manager using corresponding Scylla APIs, but in the future, the entire tablet backup/restore procedures will be handled by Scylla alone.

---

# Scylla side design

- [Tablet-aware restore design](https://scylladb.atlassian.net/wiki/spaces/RND/pages/214106113) (also contains future plans around backup)  
- [Tablet restore REST API design](https://scylladb.atlassian.net/browse/SCYLLADB-197)

## Snapshot changes ([spec](https://scylladb.atlassian.net/wiki/spaces/RND/pages/214106113/Tablet-aware+Backup+and+Restore#Scylla-manifest.json-format))

Calling general Scylla snapshot API results in creating hard links to sstables, but also in creation of `manifest.json` and `schema.cql` files. Scylla manifest will be extended to contain information needed by Scylla when performing Tablet restore. Note that Scylla manifest are per node per table while SM manifests are per node.

Scylla Tablet-aware design page mentions future plans around making Scylla snapshot a synchronized, cluster wide operation, but that’s outside of the context of MS1 - we will continue to use the existing snapshot API with its extended manifest.

## Restore changes ([spec](https://scylladb.atlassian.net/browse/SCYLLADB-197))

Scylla will expose new Tablet restore API endpoint:

- accepts keyspace and table params allowing to restore multiple keyspaces/tables with a single API call  
- accepts a list of backup location specifications (dc/object_storage_endpoint/bucket), each containing full list of Scylla manifests paths that should be restored  
- assumes that sstables listed in provided Scylla manifest are stored in the same directory as the manifest  
- assumes that DC/rack names are the same in backed up and restored cluster (node count can differ) (at least for MS1)  
- assumes that backed up table was replicated with tablets (don’t use Tablet restore for migrating from vnodes to tablets - at least in MS1)  
- works asynchronously by returning task ID that can be used when interacting with Scylla Task Manager API  

A single call to this endpoint should result in restoration of an entire (perhaps multiple) table. No batching should be performed on SM side.

This endpoint aims to leverage file based streaming (instead of mutation based one). File based streaming requires restored sstables must not cross tablet boundaries, as otherwise their contents would need to be split to mutations and sent to different nodes. In order to achieve such state, Scylla will change restored table tablet options during Tablet restore, so that the used underlying tablet map meets mentioned requirement. Scylla will also be responsible for rolling back those changes once the Scylla Tablet restore task finishes. No playing with tablet schema options or tablet load balancing should be performed on SM side.

This endpoint distributes data differently to how SM Rclone or Native restore does with cluster wide primary_replica_only streaming. It aims to stream each data replica to different replica node and shard. Because of that, we no longer need repair to distribute restored data from primary replica to other replicas. On the other hand, repair is still needed, as we still lack synchronized cluster wide snapshot API, so we need to ensure, that the state from the newest snapshot is propagated to all the replicas. Moreover, repairing is safer, as it alleviates problems that can arise when RF was changed after the data was snapshot-ed, but before table schema was described. Repair should still be performed on SM side.

---

# SM side design

## API design

There are two ways of exposing SM Tablet restore feature to the users:

- create new task type - results in creation of new REST API endpoints and sctool commands  
- extending general restore task `--method` flag with `tablet` value  

In my opinion, the second option gives us way better UX.

In the perfect world, user just runs some restore command and it chooses the best way to restore specified backup. In general, different types of restore require different prerequisites and the same goes for Tablet Aware restore (tablets needs to be used, DC/rack names must stay the same). Sometimes those requirements are met for one table, but not the other. When we want to restore both of them, we need to use different restore methods. If we were to introduce SM Tablet restore as a new task type (similar to 1-1-restore), this would mean that user is responsible for calling sctool Tablet restore command and the regular restore command and determining which tables should be restored with which one. On the other hand, if we were to introduce SM Tablet restore as new `--method` flag value, we can also modify the `auto` value to prefer Tablet restore whenever possible, which would take care of this problem.

Modifying `--method=auto` comes with one problematic scenario. The current semantic of `--method=native` is that all sstables need to be uploaded with native method. Restore fails if that’s not possible. Because of that we don’t recommend using `--method=native` in production where we might have some leftover sstables with integer based IDs. We recommend `--method=auto` which prefers native upload and allows fallback to the rclone upload for not applicable sstables. If we were to just modify `--method=auto` to prefer Tablet Aware, then native, then rclone restore, this would make it impossible to just use native restore with fallback to rclone. This could be useful when encountering some Tablet Aware restore bug.

To overcome that, we should not only extend `--method=auto`, but also allow to specify a list of methods used for the restore like `--method=native,rclone`. In this case, SM would still prioritize Tablet Aware, then native, then rclone, but it would be possible to chose all allowed restore methods.

## Backup changes

Tablet restore aims to utilize the same Scylla snapshot API and work well with both Rclone and Native backup methods. Small changes are required to achieve this state.

Currently, when indexing snapshot-ed files, SM removes `manifest.json` and `schema.cql` files before uploading the whole directory to backup location. Those files were never used by SM nor Scylla during restore, so there wasn’t any value in backing them up. Tablet restore changes that, as Scylla needs Scylla manifests for Tablet restore purposes. Note that Scylla manifests are per node per table, so we can’t simply put them directly next to backed up sstables, as this would result in file name conflicts. To overcome that, SM needs to rename Scylla manifests on upload.

To accommodate that, SM needs to:

- not remove `manifest.json` when indexing snapshot-ed fiels  
- upload(move) and rename `manifest.json` to `<snapshot_tag>_<node_ID>_manifest.json` (unique name assuming that we store manifests from different tables in different dirs) before uploading the sstables (all end up in the same backup dir)  
- extend SM manifest (per node) with a list of renamed Scylla manifests (per node per table)  
- extend SM manifest with information whether given table is replicated with tablets or vnodes (it’s useful on its own, but more importantly, in the context of MS1, we don’t want to test vnode to tablet migration made with Tablet Aware restore)  
- extend SM manifest with scylla version information (older scylla manifests can’t be used for Tablet Aware restore purposes)  
- include Scylla manifests in the backup retention procedure (purge all manifests which snapshot tags are not kept)  

## Restore changes

Not all tables are eligible for Tablet aware restore API. At the beginning of restore task, SM should check which tables meet those criteria:

- backed up cluster and restore target cluster have the same DC/racks  
- restored SM manifests contain info about Scylla manifests (it shows that backup is new enough in terms of used SM and Scylla versions to be used with Tablet restore)  
- restored table is replicated with tablets in both backup (info stored in SM manifest) and restore target cluster  
- restored table does not use sstables with integer based IDs  

### Views considerations

Tablet restore does not impact view restoration, so we still need to drop and recreate them.

### Data restoration considerations

Note that SM will need to restore tables with both Tablet aware API and rclone/native API in the context of a single restore task. In the initial iteration, this will happen sequentially. If we believe that there are any performance gains to be achieved from running those two in parallel and that such procedure is safe, we might reconsider it in the future.

Entire table can be restored with a single API call without any sstable batching on SM side. Note that even though Scylla API allows for restoring multiple tables with a single API call, SM should still restore 1 table per API call, as it allows for more granular progress tracking and better retries. Having said that, all tables should be restored in parallel, as Scylla is responsible for throttling this on its side. This changes the current node=worker approach, where each node requests batches of sstables from “random“ table/backed up node to restore. Instead, SM should simply restore each table in parallel (in some way, worker=table with a single job/batch to perform) and report on table progress (obtained via Task Manager API).

Implementation wise, we should have a separate restore stage level method (similar to `stageRestoreData`) for restoring tables with Tablet restore API. We can think about making it an actual restore stage returned to user as a part of restore progress, but it’s not necessary and could bring more confusion if documented poorly. This method could be called `tabletAwareRestore`.

We should have a separate method which restores a specific table like `tabletAwareRestoreTable`. This method would simply:

- start restoration of [eligible table](https://scylladb.atlassian.net/wiki/spaces/SC/pages/237928765/Tablet-aware+restore+MS1#Restore-changes) (checked at the beginning of restore task) with Tablet restore API (single tablet restore task per table)  
- long poll those task progress and update progress state in SM DB (similar to how native restore does so)  
- abort those tasks in case of SM task interruption  

All calls to `tabletAwareRestoreTable` should be executed in parallel and synchronized with `errgroup.Group`.

There is the matter of adjusting/reusing current metrics and SM DB representations for both pause/resume and progress reporting purposes. The general case is that often times they contain information on host level. This level of granularity is incompatible with Tablet aware restore which works on the whole cluster level (in terms of hosts) or just on a single table level. I believe that it’s better not to artificially populate metrics or progress with host level information, as this could bring more confusion than good. We should still make it simple for the user to check which restore method was used when restoring given table.

#### Metrics

In terms of metrics (prefixed with `scylla_manager_restore_`), they look like this:

```go
RestoreMetrics{
    batchSize: g("Cumulative size of the batches of files taken by the host to restore the data.", "batch_size", "cluster", "host"),
    remainingBytes: g("Remaining bytes of backup to be restored yet.", "remaining_bytes",
        "cluster", "snapshot_tag", "location", "dc", "node", "keyspace", "table"),
    state:            g("Defines current state of the restore process (idle/download/load/error).", "state", "cluster", "location", "snapshot_tag", "host"),
    restoredBytes:    g("Restored bytes", "restored_bytes", "cluster", "host"),
    restoreDuration:  g("Restore duration in ms", "restore_duration", "cluster", "host"),
    downloadedBytes:  g("Downloaded bytes", "downloaded_bytes", "cluster", "location", "host"),
    downloadDuration: g("Download duration in ms", "download_duration", "cluster", "location", "host"),
    streamedBytes:    g("Load&Streamed bytes", "streamed_bytes", "cluster", "host"),
    streamDuration:   g("Load&Stream duration in ms", "stream_duration", "cluster", "host"),
    progress:         g("Defines current progress of the restore process.", "progress", "cluster", "snapshot_tag"),
    viewBuildStatus:  g("Defines build status of recreated view.", "view_build_status", "cluster", "keyspace", "view"),
}
```

Most of them are labeled with `host` which doesn’t make sense for Tablet aware restore, where host on which the Tablet aware task was scheduled is not the host performing all of the work:

- `progress` and `remaining_bytes` (not labeled with `host`) metrics make sense to be used - they should also provide good initial visibility for Tablet aware restore  
- `state` (labeled with `host`) metric was initially introduced to measure time spent on download vs streaming part of Rclone restore. It could also be used to analyze the performance of SM side batching mechanism. Those reasons don’t apply to Tablet aware restore, so it doesn’t make sense to be used  
- `batch_size` (labeled with `host`) metric doesn’t make sense for Tablet aware restore, as we don’t batch anything on SM side  
- The family of `[downloaded|streamed|restored][bytes|duration]` metrics (labeled with `host`) is also less interesting, as Scylla metrics should be the place for tracking Tablet aware restore performance, so it doesn’t make sense to be used  

We can consider adding a new metric `method` metric labeled with `cluster`, `keyspace`, `table`. It would make it more clear which table is restored “primarily” in which way. I used “primarily“ because there is a possibility of using a mix of primarily native restore and rclone restore for leftover batches incompatible with native restore. In such cases, we can set `method` to the one which was used for restoring the majority of restored table data (updated on the fly). We wouldn’t have such a problem with Tablet aware method, as we can either restore an entire table with it, or use different methods for doing so.

#### SM DB progress representation

In terms of SM DB representation, `scylla_manager.restore_run_progress` looks like this:

```go
type RunProgress struct {
    ClusterID uuid.UUID
    TaskID    uuid.UUID
    RunID     uuid.UUID

    RemoteSSTableDir string `db:"remote_sstable_dir"`
    Keyspace         string `db:"keyspace_name"`
    Table            string `db:"table_name"`
    Host             string // IP of the node which restores the SSTables.
    // Downloading SSTables could be done by either Rclone or Scylla API.
    // In case of Scylla API, it also streams the sstables into the cluster.
    AgentJobID   int64  `db:"agent_job_id"`
    ScyllaTaskID string `db:"scylla_task_id"`

    SSTableID          []string `db:"sstable_id"`
    RestoreStartedAt   *time.Time
    RestoreCompletedAt *time.Time
    // DownloadStartedAt and DownloadCompletedAt are within the
    // RestoreStartedAt and RestoreCompletedAt time frame.
    // They are set only when Rclone API is used, because
    // Scylla API downloads and restores SSTables as a single call.
    DownloadStartedAt   *time.Time
    DownloadCompletedAt *time.Time
    Restored            int64
    Downloaded          int64
    VersionedDownloaded int64
    Failed              int64
    Error               string
    ShardCnt            int64 // Host shard count used for bandwidth per shard calculation.
}
```

It contains more columns than Tablet aware restore needs, as it only requires `ClusterID`, `TaskID`, `RunID`, `Keyspace`, `Table`, `ScyllaTaskID`, `RestoreStartedAt`, `RestoreCompletedAt`, `Restored`, `Error` columns. Even though the `RemoteSSTTableDir` and `Host` columns are a part of the clustering key, it doesn’t make sense to set them. Having said that, overloading the same run progress definition with multiple types of restore methods might be confusing to work with. Because of that, we should add a new column `Method` which would make it explicit which restore method is used in the context of given restore progress definition, instead of checking if given combination of fields is set. With such addition, it should be simple to populate `scylla_manager.restore_run_progress` with Tablet aware restore information needed for progress display and pause/resume functionality.

In terms of resume, note that since we have way smaller granularity with only a single job per entire table, we should continue waiting on the already started and running Scylla tasks from the previous run. Even though this doesn’t benefit us in the regular flow of pause/resume where Scylla tasks are aborted on pause, it does bring us value in situations where SM server was restarted due to unexpected crash. Not only does it make SM not repeat the same work twice, it also saves us from errors coming from trying to restore the same table twice in parallel, which is not (and shouldn’t be) supported by Scylla.

`scylla_manager.restore_run` does not require any adjustments.

#### Swagger progress representation

In terms of swagger progress representation, `RestoreProgress` looks like this:

```json
"RestoreProgress": {
  "type": "object",
  "properties": {
    "snapshot_tag": {
      "type": "string"
    },
    "keyspaces": {
      "type": "array",
      "items": {
        "$ref": "#/definitions/RestoreKeyspaceProgress"
      }
    },
    "hosts": {
      "type": "array",
      "items": {
        "$ref": "#/definitions/RestoreHostProgress"
      }
    },
    "views": {
      "type": "array",
      "items": {
        "$ref": "#/definitions/RestoreViewProgress"
      }
    },
    ...
  }
},
"RestoreKeyspaceProgress": {
  "type": "object",
  "properties": {
    "keyspace": {
      "type": "string"
    },
    "tables": {
      "type": "array",
      "items": {
        "$ref": "#/definitions/RestoreTableProgress"
      }
    },
    ...
  }
},
"RestoreTableProgress": {
  "type": "object",
  "properties": {
    "table": {
      "type": "string"
    },
    "tombstone_gc": {
      "type": "string"
    },
    "error": {
      "type": "string"
    },
    "size": {
      "type": "integer"
    },
    "restored": {
      "type": "integer"
    },
    "downloaded": {
      "description": "This field is DEPRECATED. Total bytes downloaded from table (included in restored)",
      "type": "integer"
    },
    "failed": {
      "type": "integer"
    },
    "started_at": {
      "type": "string",
      "format": "date-time",
      "x-nullable": true
    },
    "completed_at": {
      "type": "string",
      "format": "date-time",
      "x-nullable": true
    }
  }
},
```

As in the case of `scylla_manager.restore_run_progress`, it doesn’t make much sense to fill `RestoreHostProgress` for Tablet Aware restore, as again, the host on which Tablet aware restore task is scheduled is not the host which performs all the work. Because of that tables restored with Tablet aware restore will not contribute to `RestoreHostProgress`. On the other hand, it’s still possible that within a single SM restore task, some tables will be restored with rclone/native method, and some with Tablet aware method. To make it more comprehensive on the `sctool` side, we should add new `Method` field to `RestoreTableProgress` definition, so that we can easily group and display tables restored with Tablet aware restore in a slightly different way than than tables restored with other means. It’s generally interesting information that we could add to the `sctool` display, even if it was just for the rclone vs native restore sake. Since it’s possible for a single table to be restored with both rclone and native method, we should set the primary one - meaning the one which was used for restoring the majority of restored table data.

### Repair considerations

Tablet tables repair is still needed to overcome any inconsistencies in the snapshot-ed data.

## Testing

Assuming that we go with extending `--method` flag, it should be easy to copy/adjust existing SM repo tests to test this feature. Similar for tests in SCT.
```
