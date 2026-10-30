# Restore

The restore commands allow you to create and update a restore (ad-hoc or scheduled).

<a id="sctool-restore"></a>

## restore

<!-- -*- mode: rst -*- -->

This command allows you to run an ad-hoc restore.
Restore is always one of two types: restore schema (’–restore-schema’ flag) or restore tables’ contents (’–restore-tables’ flag).
In both cases, for the restore effects to be visible, you need to perform
a specific follow-up action described by selected type.

### Syntax

```none
sctool restore --cluster <id|name> --location [<dc>:]<provider>:<bucket> --snapshot-tag <tag> [flags]
```

### Command options

#### `--allow-compaction`

Defines if auto compactions should be running on Scylla nodes during restore.
Disabling auto compactions decreases restore time duration, but increases compaction workload after the restore is done.

**Default value:** `false`

#### `--batch-size`

Number of SSTables per shard to process in one request by one node when restoring with rclone API (see –method flag).
When restoring with native API, –batch-size is ignored and batches are as large as possible while keeping equal workload distribution across all nodes.
Increasing the default batch size might significantly increase restore performance, as only one shard can work on restoring a single SSTable bundle.
Set to 0 for best performance (batches will contain sstables of total size up to 5% of expected total node workload).

**Default value:** `2`

#### `-c, --cluster`

The target cluster name or ID (envvar SCYLLA_MANAGER_CLUSTER).

#### `--cron`

Task schedule as a cron expression.
It supports the extended syntax including @monthly, @weekly, @daily, @midnight, @hourly, @every X[h|m|s].

#### `--dc-mapping`

Specifies mapping between DCs from the backup and DCs in the restored(target) cluster.

The Syntax is “source_dc1=target_dc1,source_dc2=target_dc2” where multiple mappings are separated by comma (,)
and source and target DCs are separated by equal (=).

Example: “dc1=dc3,dc2=dc4” - data from dc1 should be restored to dc3 and data from dc2 should be restored to dc4.

Only works with tables restoration (–restore-tables=true). 
Note: Only DCs that are provided in mappings will be restored.

**Default value:** `[]`

#### `--dry-run`

Validates and displays restore information without actually running the restore.
This allows you to display what will happen should the restore run with the parameters you set.

**Default value:** `false`

#### `--enabled`

Not enabled tasks are not executed and are hidden from the task list.

**Default value:** `true`

#### `-h, --help`

help for restore

**Default value:** `false`

#### `-i, --interval`

–interval is deprecated, please use –cron instead

Time after which a successfully completed task would be run again. The supported units are:

* ‘d’ - days
* ‘h’ - hours
* ‘m’ - minutes
* ‘s’ - seconds
* ‘ms’ - milliseconds

The task run date is aligned with ‘–start date’ value.
For example, if you select ‘–interval 7d’ task would run weekly at the ‘–start-date’ time.

#### `-K, --keyspace`

A list of glob patterns separated by a comma used to include or exclude tables.
The patterns match keyspaces and tables, separate the keyspace name from the table name with a dot e.g. ‘keyspace,!keyspace.table_prefix_\*’.
The following syntax for glob patterns is supported:

* ‘\*’ - matches any number of any characters including none
* ‘?’ - matches any single character
* ‘[abc]’ - matches one character given in the bracket
* ‘[a-z]’ - matches one character from the range given in the bracket

Patterns are evaluated from left to right.
If a pattern starts with ‘!’ it unselects items that were selected by previous patterns i.e. ‘a?,!aa’ selects *ab* but not *aa*.

**Default value:** `[]`

#### `--label`

A comma-separated list of label modifications. Labels are represented as a key-value store.
Character ‘=’ has a special meaning and cannot be a part of label’s key nor value.
A single modification takes form of:
\* ‘<key>=<value>’ - sets the label <key> to <value>
\* ‘<key>-’        - removes the label

For example, ‘–label k1=v1,k2-’ will set the label ‘k1’ to ‘v1’ and will also remove label ‘k2’.

#### `-L, --location`

A list of backup locations separated by a comma, specifies places where restored backup is stored.

The format is ‘[<dc>:]<provider>:<bucket>’.
The ‘<dc>’ parameter is optional. It allows you to specify the datacenter whose nodes will be used to restore the data
from this location in a multi-dc setting, it must match Scylla nodes datacenter.
By default, all live nodes are used to restore data from specified locations.
If ‘–dc-mapping’ is used, then ‘<dc>’ parameter will be ignored.

Note that specifying datacenters closest to backup locations might reduce download time of restored data.
The supported storage ‘<provider>’s are ‘azure’, ‘gcs’, ‘s3’, ‘localstorage’.
The ‘<bucket>’ parameter is a bucket name, it must be an alphanumeric string and **may contain a dash and or a dot, but other characters are forbidden**.

**Default value:** `[]`

#### `--method`

Control native restore method (See [https://manager.docs.scylladb.com/stable/restore/native-restore](https://manager.docs.scylladb.com/stable/restore/native-restore)):

* ‘auto’: Use the supported native restore functionalities, otherwise use rclone restore.
* ‘native’: Use the native restore functionalities only.
* ‘rclone’: Use the rclone restore functionalities only.

Using native restore functionalities requires additional configuration and has its own limitations,
which are described in detail in [https://manager.docs.scylladb.com/stable/restore/native-restore](https://manager.docs.scylladb.com/stable/restore/native-restore).

**Default value:** `rclone`

#### `--name`

Task name that can be used instead of ID.

#### `-r, --num-retries`

Number of times a task reruns following a failure.

**Default value:** `3`

#### `--parallel`

The maximum number of Scylla restore jobs that can be run at the same time (on different SSTables).
Each node can take part in at most one restore at any given moment.

**Default value:** `0`

#### `--rate-limit`

Limits the download rate (as expressed in  megabytes (MiB) per second) at which sstables can be downloaded from backup location to Scylla nodes.
You can set limits for more than one DC using a comma-separated list expressed in the format [<dc>:]<limit>.
The <dc>: part is optional and is only needed when different datacenters require different download limits.
Set to 0 for no limit (default 0).

**Default value:** `[]`

#### `--restore-schema`

Specifies restore type (alternative to ‘–restore-tables’ flag).
Restore will recreate schema by applying the backed up output of DESCRIBE SCHEMA WITH INTERNALS via CQL.
It requires that restored keyspaces aren’t present in the cluster.
For the full list of prerequisites, please see [https://manager.docs.scylladb.com/stable/restore/restore-schema.html](https://manager.docs.scylladb.com/stable/restore/restore-schema.html).

**Default value:** `false`

#### `--restore-tables`

Specifies restore type (alternative to ‘–restore-schema’ flag).
Restore will recreate contents of tables specified by ‘–keyspace’ flag.
It requires that correct schema of restored tables is already present in the cluster (schema can be restored using ‘–restore-schema’ flag).
Moreover, in order to prevent situation in which current tables’ contents overlaps restored data,
tables should be truncated before initializing restore.
For the full list of prerequisites, please see [https://manager.docs.scylladb.com/stable/restore/restore-tables.html](https://manager.docs.scylladb.com/stable/restore/restore-tables.html).

**Default value:** `false`

#### `--retry-wait`

Initial exponential backoff duration X[h|m|s].
With –retry-wait 10m task will wait 10 minutes, 20 minutes and 40 minutes after first, second and third consecutire failure.

**Default value:** `10m`

#### `--show-tables`

Prints table names together with keyspace, used in combination with –dry-run.

**Default value:** `false`

#### `-T, --snapshot-tag`

Scylla Manager snapshot tag identifying restored backup.
Snapshot tags can be obtained from backup listing (‘./sctool backup list’ command - e.g. sm_20060102150405UTC).

#### `-s, --start-date`

The date can be expressed relatively to now or as a RFC3339 formatted string.
To run the task in 2 hours use ‘now+2h’. The supported units are:

* ‘d’ - days
* ‘h’ - hours
* ‘m’ - minutes
* ‘s’ - seconds
* ‘ms’ - milliseconds

If you want the task to start at a specified date use RFC3339 formatted string i.e. ‘2018-01-02T15:04:05-07:00’.
If you want the repair to start immediately, use the value ‘now’ or skip this flag.

#### `--timezone`

Timezone of –cron and –window flag values.
The default value is taken from this system, namely ‘TZ’ envvar or ‘/etc/localtime’ file.

**Default value:** `UTC`

#### `--transfers`

Sets the amount of file transfers to run in parallel when downloading files from backup location to Scylla node.
Set to 0 for the fastest download (results in setting transfers to 2\*node_shard_count).
Set to -1 for using the transfers value defined in node’s ‘scylla-manager-agent.yaml’ config file.

**Default value:** `0`

#### `--unpin-agent-cpu`

Defines if ScyllaDB Manager Agent should be unpinned from CPUs during restore.
This might significantly improve download speed at the cost of decreasing streaming speed.

**Default value:** `false`

#### `--window`

A comma-separated list of time markers in a form [WEEKDAY-]HH:MM.
WEEKDAY can be written as the whole word or only using the first 3 characters, HH:MM is an hour from 00:00 to 23:59.

* ‘MON-00:00,FRI-15:00’ - can be executed from Monday to Friday 3PM
* ‘23:00,06:00’ - can be executed every night from 11PM to 6AM
* ‘23:00,06:00,SAT-00:00,SUN-23:59’ - can be executed every night from 11PM to 6AM and all day during the weekend

**Default value:** `[]`

`--api-cert-file`

File path to HTTPS client certificate used to access the Scylla Manager server when client certificate validation is enabled (envvar SCYLLA_MANAGER_API_CERT_FILE).

`--api-key-file`

File path to HTTPS client key associated with –api-cert-file flag (envvar SCYLLA_MANAGER_API_KEY_FILE).

`--api-url`

Base URL of Scylla Manager server (envvar SCYLLA_MANAGER_API_URL).

If running sctool on the same machine as server, it’s generated based on ‘/etc/scylla-manager/scylla-manager.yaml’ file.

**Default value:** `http://127.0.0.1:5080/api/v1`

<a id="restore-update"></a>

## restore update

<!-- -*- mode: rst -*- -->

This command allows you to modify properties of an already existing restore task.
If there is one restore task the ‘restore/task-id’ argument is not needed.

### Syntax

```none
sctool restore update --cluster <id|name> [flags] [<restore/task-id>]
```

### Command options

#### `--allow-compaction`

Defines if auto compactions should be running on Scylla nodes during restore.
Disabling auto compactions decreases restore time duration, but increases compaction workload after the restore is done.

**Default value:** `false`

#### `--batch-size`

Number of SSTables per shard to process in one request by one node when restoring with rclone API (see –method flag).
When restoring with native API, –batch-size is ignored and batches are as large as possible while keeping equal workload distribution across all nodes.
Increasing the default batch size might significantly increase restore performance, as only one shard can work on restoring a single SSTable bundle.
Set to 0 for best performance (batches will contain sstables of total size up to 5% of expected total node workload).

**Default value:** `2`

#### `-c, --cluster`

The target cluster name or ID (envvar SCYLLA_MANAGER_CLUSTER).

#### `--cron`

Task schedule as a cron expression.
It supports the extended syntax including @monthly, @weekly, @daily, @midnight, @hourly, @every X[h|m|s].

#### `--dc-mapping`

Specifies mapping between DCs from the backup and DCs in the restored(target) cluster.

The Syntax is “source_dc1=target_dc1,source_dc2=target_dc2” where multiple mappings are separated by comma (,)
and source and target DCs are separated by equal (=).

Example: “dc1=dc3,dc2=dc4” - data from dc1 should be restored to dc3 and data from dc2 should be restored to dc4.

Only works with tables restoration (–restore-tables=true). 
Note: Only DCs that are provided in mappings will be restored.

**Default value:** `[]`

#### `--dry-run`

Validates and displays restore information without actually running the restore.
This allows you to display what will happen should the restore run with the parameters you set.

**Default value:** `false`

#### `--enabled`

Not enabled tasks are not executed and are hidden from the task list.

**Default value:** `true`

#### `-h, --help`

help for update

**Default value:** `false`

#### `-i, --interval`

–interval is deprecated, please use –cron instead

Time after which a successfully completed task would be run again. The supported units are:

* ‘d’ - days
* ‘h’ - hours
* ‘m’ - minutes
* ‘s’ - seconds
* ‘ms’ - milliseconds

The task run date is aligned with ‘–start date’ value.
For example, if you select ‘–interval 7d’ task would run weekly at the ‘–start-date’ time.

#### `-K, --keyspace`

A list of glob patterns separated by a comma used to include or exclude tables.
The patterns match keyspaces and tables, separate the keyspace name from the table name with a dot e.g. ‘keyspace,!keyspace.table_prefix_\*’.
The following syntax for glob patterns is supported:

* ‘\*’ - matches any number of any characters including none
* ‘?’ - matches any single character
* ‘[abc]’ - matches one character given in the bracket
* ‘[a-z]’ - matches one character from the range given in the bracket

Patterns are evaluated from left to right.
If a pattern starts with ‘!’ it unselects items that were selected by previous patterns i.e. ‘a?,!aa’ selects *ab* but not *aa*.

**Default value:** `[]`

#### `--label`

A comma-separated list of label modifications. Labels are represented as a key-value store.
Character ‘=’ has a special meaning and cannot be a part of label’s key nor value.
A single modification takes form of:
\* ‘<key>=<value>’ - sets the label <key> to <value>
\* ‘<key>-’        - removes the label

For example, ‘–label k1=v1,k2-’ will set the label ‘k1’ to ‘v1’ and will also remove label ‘k2’.

#### `-L, --location`

A list of backup locations separated by a comma, specifies places where restored backup is stored.

The format is ‘[<dc>:]<provider>:<bucket>’.
The ‘<dc>’ parameter is optional. It allows you to specify the datacenter whose nodes will be used to restore the data
from this location in a multi-dc setting, it must match Scylla nodes datacenter.
By default, all live nodes are used to restore data from specified locations.
If ‘–dc-mapping’ is used, then ‘<dc>’ parameter will be ignored.

Note that specifying datacenters closest to backup locations might reduce download time of restored data.
The supported storage ‘<provider>’s are ‘azure’, ‘gcs’, ‘s3’, ‘localstorage’.
The ‘<bucket>’ parameter is a bucket name, it must be an alphanumeric string and **may contain a dash and or a dot, but other characters are forbidden**.

**Default value:** `[]`

#### `--method`

Control native restore method (See [https://manager.docs.scylladb.com/stable/restore/native-restore](https://manager.docs.scylladb.com/stable/restore/native-restore)):

* ‘auto’: Use the supported native restore functionalities, otherwise use rclone restore.
* ‘native’: Use the native restore functionalities only.
* ‘rclone’: Use the rclone restore functionalities only.

Using native restore functionalities requires additional configuration and has its own limitations,
which are described in detail in [https://manager.docs.scylladb.com/stable/restore/native-restore](https://manager.docs.scylladb.com/stable/restore/native-restore).

**Default value:** `rclone`

#### `--name`

Task name that can be used instead of ID.

#### `-r, --num-retries`

Number of times a task reruns following a failure.

**Default value:** `3`

#### `--parallel`

The maximum number of Scylla restore jobs that can be run at the same time (on different SSTables).
Each node can take part in at most one restore at any given moment.

**Default value:** `0`

#### `--rate-limit`

Limits the download rate (as expressed in  megabytes (MiB) per second) at which sstables can be downloaded from backup location to Scylla nodes.
You can set limits for more than one DC using a comma-separated list expressed in the format [<dc>:]<limit>.
The <dc>: part is optional and is only needed when different datacenters require different download limits.
Set to 0 for no limit (default 0).

**Default value:** `[]`

#### `--restore-schema`

Specifies restore type (alternative to ‘–restore-tables’ flag).
Restore will recreate schema by applying the backed up output of DESCRIBE SCHEMA WITH INTERNALS via CQL.
It requires that restored keyspaces aren’t present in the cluster.
For the full list of prerequisites, please see [https://manager.docs.scylladb.com/stable/restore/restore-schema.html](https://manager.docs.scylladb.com/stable/restore/restore-schema.html).

**Default value:** `false`

#### `--restore-tables`

Specifies restore type (alternative to ‘–restore-schema’ flag).
Restore will recreate contents of tables specified by ‘–keyspace’ flag.
It requires that correct schema of restored tables is already present in the cluster (schema can be restored using ‘–restore-schema’ flag).
Moreover, in order to prevent situation in which current tables’ contents overlaps restored data,
tables should be truncated before initializing restore.
For the full list of prerequisites, please see [https://manager.docs.scylladb.com/stable/restore/restore-tables.html](https://manager.docs.scylladb.com/stable/restore/restore-tables.html).

**Default value:** `false`

#### `--retry-wait`

Initial exponential backoff duration X[h|m|s].
With –retry-wait 10m task will wait 10 minutes, 20 minutes and 40 minutes after first, second and third consecutire failure.

**Default value:** `10m`

#### `--show-tables`

Prints table names together with keyspace, used in combination with –dry-run.

**Default value:** `false`

#### `-T, --snapshot-tag`

Scylla Manager snapshot tag identifying restored backup.
Snapshot tags can be obtained from backup listing (‘./sctool backup list’ command - e.g. sm_20060102150405UTC).

#### `-s, --start-date`

The date can be expressed relatively to now or as a RFC3339 formatted string.
To run the task in 2 hours use ‘now+2h’. The supported units are:

* ‘d’ - days
* ‘h’ - hours
* ‘m’ - minutes
* ‘s’ - seconds
* ‘ms’ - milliseconds

If you want the task to start at a specified date use RFC3339 formatted string i.e. ‘2018-01-02T15:04:05-07:00’.
If you want the repair to start immediately, use the value ‘now’ or skip this flag.

#### `--timezone`

Timezone of –cron and –window flag values.
The default value is taken from this system, namely ‘TZ’ envvar or ‘/etc/localtime’ file.

**Default value:** `UTC`

#### `--transfers`

Sets the amount of file transfers to run in parallel when downloading files from backup location to Scylla node.
Set to 0 for the fastest download (results in setting transfers to 2\*node_shard_count).
Set to -1 for using the transfers value defined in node’s ‘scylla-manager-agent.yaml’ config file.

**Default value:** `0`

#### `--unpin-agent-cpu`

Defines if ScyllaDB Manager Agent should be unpinned from CPUs during restore.
This might significantly improve download speed at the cost of decreasing streaming speed.

**Default value:** `false`

#### `--window`

A comma-separated list of time markers in a form [WEEKDAY-]HH:MM.
WEEKDAY can be written as the whole word or only using the first 3 characters, HH:MM is an hour from 00:00 to 23:59.

* ‘MON-00:00,FRI-15:00’ - can be executed from Monday to Friday 3PM
* ‘23:00,06:00’ - can be executed every night from 11PM to 6AM
* ‘23:00,06:00,SAT-00:00,SUN-23:59’ - can be executed every night from 11PM to 6AM and all day during the weekend

**Default value:** `[]`

`--api-cert-file`

File path to HTTPS client certificate used to access the Scylla Manager server when client certificate validation is enabled (envvar SCYLLA_MANAGER_API_CERT_FILE).

`--api-key-file`

File path to HTTPS client key associated with –api-cert-file flag (envvar SCYLLA_MANAGER_API_KEY_FILE).

`--api-url`

Base URL of Scylla Manager server (envvar SCYLLA_MANAGER_API_URL).

If running sctool on the same machine as server, it’s generated based on ‘/etc/scylla-manager/scylla-manager.yaml’ file.

**Default value:** `http://127.0.0.1:5080/api/v1`

<a id="sctool-1-1-restore"></a>

## 1-1-restore

<!-- -*- mode: rst -*- -->

This command allows you to run ad-hoc restore of tables to a cluster that mirrors the topology of a backup cluster. This means that the target cluster must have 
the same number of nodes in each datacenter and rack, as well as the same token assignment for each node.

**Note:**  
: - Pefromance of 1-1-restore procedure is significantly better if Scylla version is 2025.2 or later.
  - This command works only with vnode based keyspaces. Attempting to restore tablet keyspaces will result in an error. To restore from a backup that includes both vnode and tablet keyspaces, use the –keyspace flag to select only the vnode keyspaces.
  - This command sets tombstone_gc mode to repair for the restored tables and views which is required to avoid running repair operation as part of the restore procedure. After restoration, the tombstone_gc mode can only be changed once the tables have been repaired — otherwise, data resurrection may occur.
  - This command only restores the data within the tables. You must first restore the schema of the database separately using the regular restore command with the –restore-schema flag.

### Syntax

```none
sctool restore 1-1-restore --cluster <id|name> --location [<dc>:]<provider>:<bucket> --snapshot-tag <tag> [flags] --source-cluster-id <id> --nodes-mapping <filepath>
```

### Command options

#### `-c, --cluster`

The target cluster name or ID (envvar SCYLLA_MANAGER_CLUSTER).

#### `--cron`

Task schedule as a cron expression.
It supports the extended syntax including @monthly, @weekly, @daily, @midnight, @hourly, @every X[h|m|s].

#### `--dry-run`

Validates and displays restore information without actually running the restore.
This allows you to display what will happen should the restore run with the parameters you set.

**Default value:** `false`

#### `--enabled`

Not enabled tasks are not executed and are hidden from the task list.

**Default value:** `true`

#### `-h, --help`

help for 1-1-restore

**Default value:** `false`

#### `-i, --interval`

–interval is deprecated, please use –cron instead

Time after which a successfully completed task would be run again. The supported units are:

* ‘d’ - days
* ‘h’ - hours
* ‘m’ - minutes
* ‘s’ - seconds
* ‘ms’ - milliseconds

The task run date is aligned with ‘–start date’ value.
For example, if you select ‘–interval 7d’ task would run weekly at the ‘–start-date’ time.

#### `-K, --keyspace`

A list of glob patterns separated by a comma used to include or exclude tables.
The patterns match keyspaces and tables, separate the keyspace name from the table name with a dot e.g. ‘keyspace,!keyspace.table_prefix_\*’.
The following syntax for glob patterns is supported:

* ‘\*’ - matches any number of any characters including none
* ‘?’ - matches any single character
* ‘[abc]’ - matches one character given in the bracket
* ‘[a-z]’ - matches one character from the range given in the bracket

Patterns are evaluated from left to right.
If a pattern starts with ‘!’ it unselects items that were selected by previous patterns i.e. ‘a?,!aa’ selects *ab* but not *aa*.

**Default value:** `[]`

#### `--label`

A comma-separated list of label modifications. Labels are represented as a key-value store.
Character ‘=’ has a special meaning and cannot be a part of label’s key nor value.
A single modification takes form of:
\* ‘<key>=<value>’ - sets the label <key> to <value>
\* ‘<key>-’        - removes the label

For example, ‘–label k1=v1,k2-’ will set the label ‘k1’ to ‘v1’ and will also remove label ‘k2’.

#### `-L, --location`

A list of backup locations separated by a comma, specifies places where restored backup is stored.

The format is ‘[<dc>:]<provider>:<bucket>’.
The ‘<dc>’ parameter will be ignored and nodes-mapping will be used instead.
The supported storage ‘<provider>’s are ‘azure’, ‘gcs’, ‘s3’, ‘localstorage’.
The ‘bucket’ parameter is a bucket name, it must be an alphanumeric string and **may contain a dash and or a dot, but other characters are forbidden**.

**Default value:** `[]`

#### `--name`

Task name that can be used instead of ID.

#### `--nodes-mapping`

Path to a file with source cluster and target cluster nodes mapping. Each line should contain node mapping in the following format <source_dc>:<source_rack>:<source_host_id>=<destination_dc>:<destination_rack>:<destination_host_id>

#### `-r, --num-retries`

Number of times a task reruns following a failure.

**Default value:** `3`

#### `--retry-wait`

Initial exponential backoff duration X[h|m|s].
With –retry-wait 10m task will wait 10 minutes, 20 minutes and 40 minutes after first, second and third consecutire failure.

**Default value:** `10m`

#### `-T, --snapshot-tag`

Scylla Manager snapshot tag identifying restored backup.
Snapshot tags can be obtained from backup listing (‘./sctool backup list’ command - e.g. sm_20060102150405UTC).

#### `--source-cluster-id`

Cluster ID of the backup cluster.

#### `-s, --start-date`

The date can be expressed relatively to now or as a RFC3339 formatted string.
To run the task in 2 hours use ‘now+2h’. The supported units are:

* ‘d’ - days
* ‘h’ - hours
* ‘m’ - minutes
* ‘s’ - seconds
* ‘ms’ - milliseconds

If you want the task to start at a specified date use RFC3339 formatted string i.e. ‘2018-01-02T15:04:05-07:00’.
If you want the repair to start immediately, use the value ‘now’ or skip this flag.

#### `--timezone`

Timezone of –cron and –window flag values.
The default value is taken from this system, namely ‘TZ’ envvar or ‘/etc/localtime’ file.

**Default value:** `UTC`

#### `--unpin-agent-cpu`

Defines if ScyllaDB Manager Agent should be unpinned from CPUs during restore.

**Default value:** `false`

#### `--window`

A comma-separated list of time markers in a form [WEEKDAY-]HH:MM.
WEEKDAY can be written as the whole word or only using the first 3 characters, HH:MM is an hour from 00:00 to 23:59.

* ‘MON-00:00,FRI-15:00’ - can be executed from Monday to Friday 3PM
* ‘23:00,06:00’ - can be executed every night from 11PM to 6AM
* ‘23:00,06:00,SAT-00:00,SUN-23:59’ - can be executed every night from 11PM to 6AM and all day during the weekend

**Default value:** `[]`

`--api-cert-file`

File path to HTTPS client certificate used to access the Scylla Manager server when client certificate validation is enabled (envvar SCYLLA_MANAGER_API_CERT_FILE).

`--api-key-file`

File path to HTTPS client key associated with –api-cert-file flag (envvar SCYLLA_MANAGER_API_KEY_FILE).

`--api-url`

Base URL of Scylla Manager server (envvar SCYLLA_MANAGER_API_URL).

If running sctool on the same machine as server, it’s generated based on ‘/etc/scylla-manager/scylla-manager.yaml’ file.

**Default value:** `http://127.0.0.1:5080/api/v1`

## 1-1-restore update

<!-- -*- mode: rst -*- -->

This command allows you to modify properties of an already existing 1-1-restore task.
If there is one 1-1-restore task the ‘1_1_restore/task-id’ argument is not needed.

### Syntax

```none
sctool restore 1-1-restore update --cluster <id|name> [flags] [<1_1_restore/task-id>]
```

### Command options

#### `-c, --cluster`

The target cluster name or ID (envvar SCYLLA_MANAGER_CLUSTER).

#### `--cron`

Task schedule as a cron expression.
It supports the extended syntax including @monthly, @weekly, @daily, @midnight, @hourly, @every X[h|m|s].

#### `--dry-run`

Validates and displays restore information without actually running the restore.
This allows you to display what will happen should the restore run with the parameters you set.

**Default value:** `false`

#### `--enabled`

Not enabled tasks are not executed and are hidden from the task list.

**Default value:** `true`

#### `-h, --help`

help for update

**Default value:** `false`

#### `-i, --interval`

–interval is deprecated, please use –cron instead

Time after which a successfully completed task would be run again. The supported units are:

* ‘d’ - days
* ‘h’ - hours
* ‘m’ - minutes
* ‘s’ - seconds
* ‘ms’ - milliseconds

The task run date is aligned with ‘–start date’ value.
For example, if you select ‘–interval 7d’ task would run weekly at the ‘–start-date’ time.

#### `-K, --keyspace`

A list of glob patterns separated by a comma used to include or exclude tables.
The patterns match keyspaces and tables, separate the keyspace name from the table name with a dot e.g. ‘keyspace,!keyspace.table_prefix_\*’.
The following syntax for glob patterns is supported:

* ‘\*’ - matches any number of any characters including none
* ‘?’ - matches any single character
* ‘[abc]’ - matches one character given in the bracket
* ‘[a-z]’ - matches one character from the range given in the bracket

Patterns are evaluated from left to right.
If a pattern starts with ‘!’ it unselects items that were selected by previous patterns i.e. ‘a?,!aa’ selects *ab* but not *aa*.

**Default value:** `[]`

#### `--label`

A comma-separated list of label modifications. Labels are represented as a key-value store.
Character ‘=’ has a special meaning and cannot be a part of label’s key nor value.
A single modification takes form of:
\* ‘<key>=<value>’ - sets the label <key> to <value>
\* ‘<key>-’        - removes the label

For example, ‘–label k1=v1,k2-’ will set the label ‘k1’ to ‘v1’ and will also remove label ‘k2’.

#### `-L, --location`

A list of backup locations separated by a comma, specifies places where restored backup is stored.

The format is ‘[<dc>:]<provider>:<bucket>’.
The ‘<dc>’ parameter will be ignored and nodes-mapping will be used instead.
The supported storage ‘<provider>’s are ‘azure’, ‘gcs’, ‘s3’, ‘localstorage’.
The ‘bucket’ parameter is a bucket name, it must be an alphanumeric string and **may contain a dash and or a dot, but other characters are forbidden**.

**Default value:** `[]`

#### `--name`

Task name that can be used instead of ID.

#### `--nodes-mapping`

Path to a file with source cluster and target cluster nodes mapping. Each line should contain node mapping in the following format <source_dc>:<source_rack>:<source_host_id>=<destination_dc>:<destination_rack>:<destination_host_id>

#### `-r, --num-retries`

Number of times a task reruns following a failure.

**Default value:** `3`

#### `--retry-wait`

Initial exponential backoff duration X[h|m|s].
With –retry-wait 10m task will wait 10 minutes, 20 minutes and 40 minutes after first, second and third consecutire failure.

**Default value:** `10m`

#### `-T, --snapshot-tag`

Scylla Manager snapshot tag identifying restored backup.
Snapshot tags can be obtained from backup listing (‘./sctool backup list’ command - e.g. sm_20060102150405UTC).

#### `--source-cluster-id`

Cluster ID of the backup cluster.

#### `-s, --start-date`

The date can be expressed relatively to now or as a RFC3339 formatted string.
To run the task in 2 hours use ‘now+2h’. The supported units are:

* ‘d’ - days
* ‘h’ - hours
* ‘m’ - minutes
* ‘s’ - seconds
* ‘ms’ - milliseconds

If you want the task to start at a specified date use RFC3339 formatted string i.e. ‘2018-01-02T15:04:05-07:00’.
If you want the repair to start immediately, use the value ‘now’ or skip this flag.

#### `--timezone`

Timezone of –cron and –window flag values.
The default value is taken from this system, namely ‘TZ’ envvar or ‘/etc/localtime’ file.

**Default value:** `UTC`

#### `--unpin-agent-cpu`

Defines if ScyllaDB Manager Agent should be unpinned from CPUs during restore.

**Default value:** `false`

#### `--window`

A comma-separated list of time markers in a form [WEEKDAY-]HH:MM.
WEEKDAY can be written as the whole word or only using the first 3 characters, HH:MM is an hour from 00:00 to 23:59.

* ‘MON-00:00,FRI-15:00’ - can be executed from Monday to Friday 3PM
* ‘23:00,06:00’ - can be executed every night from 11PM to 6AM
* ‘23:00,06:00,SAT-00:00,SUN-23:59’ - can be executed every night from 11PM to 6AM and all day during the weekend

**Default value:** `[]`

`--api-cert-file`

File path to HTTPS client certificate used to access the Scylla Manager server when client certificate validation is enabled (envvar SCYLLA_MANAGER_API_CERT_FILE).

`--api-key-file`

File path to HTTPS client key associated with –api-cert-file flag (envvar SCYLLA_MANAGER_API_KEY_FILE).

`--api-url`

Base URL of Scylla Manager server (envvar SCYLLA_MANAGER_API_URL).

If running sctool on the same machine as server, it’s generated based on ‘/etc/scylla-manager/scylla-manager.yaml’ file.

**Default value:** `http://127.0.0.1:5080/api/v1`
