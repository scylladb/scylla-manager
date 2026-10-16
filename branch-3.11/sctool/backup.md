# Backup

The backup commands allow you to: create and update a backup (ad-hoc or scheduled), list the contents of a backup, and list the backups of a cluster.
You cannot initiate a backup without a cluster. Make sure you add a cluster ([cluster add](https://manager.docs.scylladb.com/branch-3.11/sctool/cluster.md#cluster-add)) before initiating a backup.

<a id="sctool-backup"></a>

## backup

<!-- -*- mode: rst -*- -->

This command allows you to schedule or run ad-hoc cluster backup.

### Syntax

```none
sctool backup --cluster <id|name> --location [<dc>:]<provider>:<bucket> [--keyspace <glob>] [flags]
```

### Command options

#### `-c, --cluster`

The target cluster name or ID (envvar SCYLLA_MANAGER_CLUSTER).

#### `--cron`

Task schedule as a cron expression.
It supports the extended syntax including @monthly, @weekly, @daily, @midnight, @hourly, @every X[h|m|s].

#### `--dc`

A list of datacenter glob patterns separated by a comma, e.g. ‘dc1,!otherdc\*’ used to specify the datacenters to include or exclude.
The following syntax for glob patterns is supported:

* ‘\*’ - matches any number of any characters including none
* ‘?’ - matches any single character
* ‘[abc]’ - matches one character given in the bracket
* ‘[a-z]’ - matches one character from the range given in the bracket

Patterns are evaluated from left to right.
If a pattern starts with ‘!’ it unselects items that were selected by previous patterns i.e. ‘a?,!aa’ selects *ab* but not *aa*.

**Default value:** `[]`

#### `--dry-run`

Validates and prints backup information without actually scheduling a backup.

**Default value:** `false`

#### `--enabled`

Not enabled tasks are not executed and are hidden from the task list.

**Default value:** `true`

#### `-h, --help`

help for backup

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

A list of backup locations separated by a comma, specifies where to place the backup, the format is [<dc>:]<provider>:<bucket>.
The ‘<dc>’ parameter is optional it allows to specify location for a datacenter in a multi-dc setting, it must match Scylla nodes datacenter.
The supported storage ‘<provider>’s are ‘azure’, ‘gcs’, ‘s3’, ‘localstorage’.
The ‘bucket’ parameter is a bucket name, it must be an alphanumeric string and **may contain a dash and or a dot, but other characters are forbidden**.

**Default value:** `[]`

#### `--method`

Control native backup method (See [https://manager.docs.scylladb.com/stable/backup/native-backup](https://manager.docs.scylladb.com/stable/backup/native-backup)):

* ‘auto’: Use the supported native backup functionalities, otherwise use rclone backup.
* ‘native’: Use the native backup functionalities only.
* ‘rclone’: Use the rclone backup functionalities only.

Using native backup functionalities requires additional configuration and has its own limitations,
which are described in details in [https://manager.docs.scylladb.com/stable/backup/native-backup](https://manager.docs.scylladb.com/stable/backup/native-backup).

**Default value:** `rclone`

#### `--name`

Task name that can be used instead of ID.

#### `-r, --num-retries`

Number of times a task reruns following a failure.

**Default value:** `3`

#### `--override-retention-lock`

Override previously set retention locks in ‘unlocked’ mode.
This flag is required when changing retention lock mode to ‘locked’ or when decreasing retention period (’–retention-days’).

Using retention lock requires additional configuration and has its own limitations,
which are described in details in [https://manager.docs.scylladb.com/stable/backup/retention-lock](https://manager.docs.scylladb.com/stable/backup/retention-lock).

**Default value:** `false`

#### `--purge-only`

Run the backup cleanup only.

**Default value:** `false`

#### `--rate-limit`

Limits the upload rate (as expressed in  megabytes (MiB) per second) at which snapshotfiles can be uploaded from a Scylla node to its backup destination.
You can set limits for more than one DC using a comma-separated list expressed in the format [<dc>:]<limit>.
The <dc>: part is optional and is only needed when different datacenters require different upload limits.
Set to 0 for no limit (default 100).
Does not take effect when using ‘–method=native’ flag. See ‘–method’ docs for more details.

**Default value:** `[]`

#### `--retention`

The number of backups to store, once this number is reached, the next backup which comes in from this destination will initiate a purge of the oldest backup.
Can be used simultaneously with ‘–retention-days’ flag.

**Default value:** `7`

#### `--retention-days`

The number of days for which backup is stored, the next backup will initiate a purge of all expired backups.
Can be used simultaneously with ‘–retention’ flag.

**Default value:** `0`

#### `--retention-lock-mode`

Set object retention lock mode for snapshot files.
When enabled, snapshot files in the backup location are protected from deletion until the retention period (set by ‘–retention-days’) expires.
Supported for GCS cloud provider only.
Supported values:

* ‘disabled’: No retention lock is applied to snapshot files (default).
* ‘unlocked’: Retention lock is applied but can be shortened or removed with special permissions.
* ‘locked’: Retention lock is applied and cannot be overridden.

Requires ‘–retention-days’ to be set to a positive value.
Count-based ‘–retention’ mustn’t be set when retention lock is enabled.

Using retention lock requires additional configuration and has its own limitations,
which are described in details in [https://manager.docs.scylladb.com/stable/backup/retention-lock](https://manager.docs.scylladb.com/stable/backup/retention-lock).

**Default value:** `disabled`

#### `--retry-wait`

Initial exponential backoff duration X[h|m|s].
With –retry-wait 10m task will wait 10 minutes, 20 minutes and 40 minutes after first, second and third consecutire failure.

**Default value:** `10m`

#### `--show-tables`

Prints table names together with keyspace, used in combination with –dry-run.

**Default value:** `false`

#### `--skip-schema`

Don’t backup schema.
SM requires CQL credentials to back up the schema.
CQL Credentials can be added with ‘sctool cluster update –username –password’ command.
This flag can be used to skip this step and allow for backing up user data without providing SM with CQL credentials.
Note that it’s impossible to restore schema from such backups.

**Default value:** `false`

#### `--snapshot-parallel`

A comma-separated list of snapshot parallelism limits in the format [<dc>:]<limit>.
The ‘dc’ part is optional and allows for specifying different limits in selected datacenters.
If the ‘dc’ part is not set, the limit is global and the runs are parallel in ‘n’ nodes.
If for example, you were to set ‘dc1:2,5’, then ‘dc1’ would have two parallel nodes and there would be five parallel nodes in the other DCs.

**Default value:** `[]`

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

Sets the amount of file transfers to run in parallel when uploading files from a Scylla node to its backup location.
Set to -1 for using the transfers value defined in node’s ‘scylla-manager-agent.yaml’ config file.
Does not take effect when using ‘–method=native’ flag. See ‘–method’ docs for more details.

**Default value:** `-1`

#### `--upload-parallel`

A comma-separated list of upload parallelism limits in the format [<dc>:]<limit>.
The ‘dc’ part is optional and allows for specifying different limits in selected datacenters.
If the ‘dc’ part is not set, the limit is global (e.g. ‘dc1:2,5’) the runs are parallel in ‘n’ nodes.
If for example, you were to set ‘dc1:2,5’, then ‘dc1’ would have two parallel nodes and there would be five parallel nodes in the other DCs.

**Default value:** `[]`

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

<a id="backup-validate"></a>

## backup validate

<!-- -*- mode: rst -*- -->

This command schedules a backup validation task.
It checks that all needed files are in tact, and that there are no unexpected files occupying your storage.
To delete the unexpected files provide the `--delete-orphaned-files` parameter.
To see the validation results use [progress](https://manager.docs.scylladb.com/branch-3.11/sctool/progress.md#task-progress) command.
It is safe to run backup and backup validation at the same time.

### Syntax

```none
sctool backup validate --cluster <id|name> [--delete-orphaned-files] [flags]
```

### Command options

#### `-c, --cluster`

The target cluster name or ID (envvar SCYLLA_MANAGER_CLUSTER).

#### `--cron`

Task schedule as a cron expression.
It supports the extended syntax including @monthly, @weekly, @daily, @midnight, @hourly, @every X[h|m|s].

#### `--delete-orphaned-files`

If set data files not belonging to any snapshot will be deleted after the validation.

**Default value:** `false`

#### `--enabled`

Not enabled tasks are not executed and are hidden from the task list.

**Default value:** `true`

#### `-h, --help`

help for validate

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

#### `--label`

A comma-separated list of label modifications. Labels are represented as a key-value store.
Character ‘=’ has a special meaning and cannot be a part of label’s key nor value.
A single modification takes form of:
\* ‘<key>=<value>’ - sets the label <key> to <value>
\* ‘<key>-’        - removes the label

For example, ‘–label k1=v1,k2-’ will set the label ‘k1’ to ‘v1’ and will also remove label ‘k2’.

#### `-L, --location`

A list of backup locations separated by a comma, specifies where to place the backup, the format is [<dc>:]<provider>:<bucket>.
The ‘<dc>’ parameter is optional it allows to specify location for a datacenter in a multi-dc setting, it must match Scylla nodes datacenter.
The supported storage ‘<provider>’s are ‘azure’, ‘gcs’, ‘s3’, ‘localstorage’.
The ‘bucket’ parameter is a bucket name, it must be an alphanumeric string and **may contain a dash and or a dot, but other characters are forbidden**.

**Default value:** `[]`

#### `--name`

Task name that can be used instead of ID.

#### `-r, --num-retries`

Number of times a task reruns following a failure.

**Default value:** `3`

#### `--parallel`

Number of hosts to analyze in parallel.

**Default value:** `0`

#### `--retry-wait`

Initial exponential backoff duration X[h|m|s].
With –retry-wait 10m task will wait 10 minutes, 20 minutes and 40 minutes after first, second and third consecutire failure.

**Default value:** `10m`

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

<a id="backup-update"></a>

## backup update

<!-- -*- mode: rst -*- -->

This command allows you to modify properties of an already existing backup task.
If there is one backup task the ‘backup/<id|name>’ argument is not needed.

### Syntax

```none
sctool backup update --cluster <id|name> [flags] [backup/<id|name>]
```

### Command options

#### `-c, --cluster`

The target cluster name or ID (envvar SCYLLA_MANAGER_CLUSTER).

#### `--cron`

Task schedule as a cron expression.
It supports the extended syntax including @monthly, @weekly, @daily, @midnight, @hourly, @every X[h|m|s].

#### `--dc`

A list of datacenter glob patterns separated by a comma, e.g. ‘dc1,!otherdc\*’ used to specify the datacenters to include or exclude.
The following syntax for glob patterns is supported:

* ‘\*’ - matches any number of any characters including none
* ‘?’ - matches any single character
* ‘[abc]’ - matches one character given in the bracket
* ‘[a-z]’ - matches one character from the range given in the bracket

Patterns are evaluated from left to right.
If a pattern starts with ‘!’ it unselects items that were selected by previous patterns i.e. ‘a?,!aa’ selects *ab* but not *aa*.

**Default value:** `[]`

#### `--dry-run`

Validates and prints backup information without actually scheduling a backup.

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

A list of backup locations separated by a comma, specifies where to place the backup, the format is [<dc>:]<provider>:<bucket>.
The ‘<dc>’ parameter is optional it allows to specify location for a datacenter in a multi-dc setting, it must match Scylla nodes datacenter.
The supported storage ‘<provider>’s are ‘azure’, ‘gcs’, ‘s3’, ‘localstorage’.
The ‘bucket’ parameter is a bucket name, it must be an alphanumeric string and **may contain a dash and or a dot, but other characters are forbidden**.

**Default value:** `[]`

#### `--method`

Control native backup method (See [https://manager.docs.scylladb.com/stable/backup/native-backup](https://manager.docs.scylladb.com/stable/backup/native-backup)):

* ‘auto’: Use the supported native backup functionalities, otherwise use rclone backup.
* ‘native’: Use the native backup functionalities only.
* ‘rclone’: Use the rclone backup functionalities only.

Using native backup functionalities requires additional configuration and has its own limitations,
which are described in details in [https://manager.docs.scylladb.com/stable/backup/native-backup](https://manager.docs.scylladb.com/stable/backup/native-backup).

**Default value:** `rclone`

#### `--name`

Task name that can be used instead of ID.

#### `-r, --num-retries`

Number of times a task reruns following a failure.

**Default value:** `3`

#### `--override-retention-lock`

Override previously set retention locks in ‘unlocked’ mode.
This flag is required when changing retention lock mode to ‘locked’ or when decreasing retention period (’–retention-days’).

Using retention lock requires additional configuration and has its own limitations,
which are described in details in [https://manager.docs.scylladb.com/stable/backup/retention-lock](https://manager.docs.scylladb.com/stable/backup/retention-lock).

**Default value:** `false`

#### `--purge-only`

Run the backup cleanup only.

**Default value:** `false`

#### `--rate-limit`

Limits the upload rate (as expressed in  megabytes (MiB) per second) at which snapshotfiles can be uploaded from a Scylla node to its backup destination.
You can set limits for more than one DC using a comma-separated list expressed in the format [<dc>:]<limit>.
The <dc>: part is optional and is only needed when different datacenters require different upload limits.
Set to 0 for no limit (default 100).
Does not take effect when using ‘–method=native’ flag. See ‘–method’ docs for more details.

**Default value:** `[]`

#### `--retention`

The number of backups to store, once this number is reached, the next backup which comes in from this destination will initiate a purge of the oldest backup.
Can be used simultaneously with ‘–retention-days’ flag.

**Default value:** `7`

#### `--retention-days`

The number of days for which backup is stored, the next backup will initiate a purge of all expired backups.
Can be used simultaneously with ‘–retention’ flag.

**Default value:** `0`

#### `--retention-lock-mode`

Set object retention lock mode for snapshot files.
When enabled, snapshot files in the backup location are protected from deletion until the retention period (set by ‘–retention-days’) expires.
Supported for GCS cloud provider only.
Supported values:

* ‘disabled’: No retention lock is applied to snapshot files (default).
* ‘unlocked’: Retention lock is applied but can be shortened or removed with special permissions.
* ‘locked’: Retention lock is applied and cannot be overridden.

Requires ‘–retention-days’ to be set to a positive value.
Count-based ‘–retention’ mustn’t be set when retention lock is enabled.

Using retention lock requires additional configuration and has its own limitations,
which are described in details in [https://manager.docs.scylladb.com/stable/backup/retention-lock](https://manager.docs.scylladb.com/stable/backup/retention-lock).

**Default value:** `disabled`

#### `--retry-wait`

Initial exponential backoff duration X[h|m|s].
With –retry-wait 10m task will wait 10 minutes, 20 minutes and 40 minutes after first, second and third consecutire failure.

**Default value:** `10m`

#### `--show-tables`

Prints table names together with keyspace, used in combination with –dry-run.

**Default value:** `false`

#### `--skip-schema`

Don’t backup schema.
SM requires CQL credentials to back up the schema.
CQL Credentials can be added with ‘sctool cluster update –username –password’ command.
This flag can be used to skip this step and allow for backing up user data without providing SM with CQL credentials.
Note that it’s impossible to restore schema from such backups.

**Default value:** `false`

#### `--snapshot-parallel`

A comma-separated list of snapshot parallelism limits in the format [<dc>:]<limit>.
The ‘dc’ part is optional and allows for specifying different limits in selected datacenters.
If the ‘dc’ part is not set, the limit is global and the runs are parallel in ‘n’ nodes.
If for example, you were to set ‘dc1:2,5’, then ‘dc1’ would have two parallel nodes and there would be five parallel nodes in the other DCs.

**Default value:** `[]`

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

Sets the amount of file transfers to run in parallel when uploading files from a Scylla node to its backup location.
Set to -1 for using the transfers value defined in node’s ‘scylla-manager-agent.yaml’ config file.
Does not take effect when using ‘–method=native’ flag. See ‘–method’ docs for more details.

**Default value:** `-1`

#### `--upload-parallel`

A comma-separated list of upload parallelism limits in the format [<dc>:]<limit>.
The ‘dc’ part is optional and allows for specifying different limits in selected datacenters.
If the ‘dc’ part is not set, the limit is global (e.g. ‘dc1:2,5’) the runs are parallel in ‘n’ nodes.
If for example, you were to set ‘dc1:2,5’, then ‘dc1’ would have two parallel nodes and there would be five parallel nodes in the other DCs.

**Default value:** `[]`

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

<a id="backup-list"></a>

## backup list

<!-- -*- mode: rst -*- -->

This command allow you to list backups from given locations. The command works per cluster and all the locations listed with the –location flag must be accessible from the cluster specified with the –cluster flag.
This cluster serves as the coordinator for all these checks. If the Scylla Manager Agent’s of given cluster is not able to reach the location, the command will fail.

### Syntax

```none
sctool backup list --cluster <id|name> [flags]
```

### Command options

#### `--all-clusters`

Shows backups for all clusters. Useful for listing clusters that are no longer available locally but are backed up in the past to remote location.

**Default value:** `false`

#### `-c, --cluster`

The target cluster name or ID (envvar SCYLLA_MANAGER_CLUSTER).

#### `-h, --help`

help for list

**Default value:** `false`

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

#### `-L, --location`

A list of backup locations separated by a comma, specifies where to place the backup, the format is [<dc>:]<provider>:<bucket>.
The ‘<dc>’ parameter is optional it allows to specify location for a datacenter in a multi-dc setting, it must match Scylla nodes datacenter.
The supported storage ‘<provider>’s are ‘azure’, ‘gcs’, ‘s3’, ‘localstorage’.
The ‘bucket’ parameter is a bucket name, it must be an alphanumeric string and **may contain a dash and or a dot, but other characters are forbidden**.

**Default value:** `[]`

#### `--max-date`

Specifies maximal snapshot date expressed in RFC3339 form or ‘now[+duration]’, ex. ‘now+3d2h10m’.
Valid units are:

* ‘d’ - days
* ‘h’ - hours
* ‘m’ - minutes
* ‘s’ - seconds
* ‘now’ - happens immediately

#### `--min-date`

Specifies minimal snapshot date expressed in RFC3339 form or ‘now[+duration]’, ex. ‘now+3d2h10m’.
Valid units are:

* ‘d’ - days
* ‘h’ - hours
* ‘m’ - minutes
* ‘s’ - seconds
* ‘now’ - happens immediately

#### `--show-tables`

Prints table names together with keyspace.

**Default value:** `false`

`--api-cert-file`

File path to HTTPS client certificate used to access the Scylla Manager server when client certificate validation is enabled (envvar SCYLLA_MANAGER_API_CERT_FILE).

`--api-key-file`

File path to HTTPS client key associated with –api-cert-file flag (envvar SCYLLA_MANAGER_API_KEY_FILE).

`--api-url`

Base URL of Scylla Manager server (envvar SCYLLA_MANAGER_API_URL).

If running sctool on the same machine as server, it’s generated based on ‘/etc/scylla-manager/scylla-manager.yaml’ file.

**Default value:** `http://127.0.0.1:5080/api/v1`

<a id="backup-delete"></a>

## backup delete

<!-- -*- mode: rst -*- -->

This command allows you to delete files that were uploaded during the backup procedure.
Deduplicated files are persisted unless their reference count drops to zero.
The –cluster flag passed to the command designates the owner of the snapshot.
If the –snapshot-tag does not belong to the –cluster, the command will end with an error.

### Syntax

```none
sctool backup delete --cluster <id|name> --snapshot-tag <tag> [flags]
```

### Command options

#### `-c, --cluster`

The target cluster name or ID (envvar SCYLLA_MANAGER_CLUSTER).

#### `-h, --help`

help for delete

**Default value:** `false`

#### `-L, --location`

A list of backup locations separated by a comma, specifies where to place the backup, the format is [<dc>:]<provider>:<bucket>.
The ‘<dc>’ parameter is optional it allows to specify location for a datacenter in a multi-dc setting, it must match Scylla nodes datacenter.
The supported storage ‘<provider>’s are ‘azure’, ‘gcs’, ‘s3’, ‘localstorage’.
The ‘bucket’ parameter is a bucket name, it must be an alphanumeric string and **may contain a dash and or a dot, but other characters are forbidden**.

**Default value:** `[]`

#### `-T, --snapshot-tag`

A list of snapshot tags separated by a comma.

**Default value:** `[]`

`--api-cert-file`

File path to HTTPS client certificate used to access the Scylla Manager server when client certificate validation is enabled (envvar SCYLLA_MANAGER_API_CERT_FILE).

`--api-key-file`

File path to HTTPS client key associated with –api-cert-file flag (envvar SCYLLA_MANAGER_API_KEY_FILE).

`--api-url`

Base URL of Scylla Manager server (envvar SCYLLA_MANAGER_API_URL).

If running sctool on the same machine as server, it’s generated based on ‘/etc/scylla-manager/scylla-manager.yaml’ file.

**Default value:** `http://127.0.0.1:5080/api/v1`

<a id="backup-delete-local-snapshots"></a>

## backup delete local-snapshots

<!-- -*- mode: rst -*- -->

This command removes local snapshots made by ScyllaDB Manager from nodes’ disks.
It does not delete any files stored in the backup location.
Before running this command, ensure that all backup tasks are stopped.
After deletion, any interrupted backups should be started from scratch.

### Syntax

```none
sctool backup delete local-snapshot --cluster <id|name> [flags]
```

### Command options

#### `-c, --cluster`

The target cluster name or ID (envvar SCYLLA_MANAGER_CLUSTER).

#### `-h, --help`

help for local-snapshot

**Default value:** `false`

`--api-cert-file`

File path to HTTPS client certificate used to access the Scylla Manager server when client certificate validation is enabled (envvar SCYLLA_MANAGER_API_CERT_FILE).

`--api-key-file`

File path to HTTPS client key associated with –api-cert-file flag (envvar SCYLLA_MANAGER_API_KEY_FILE).

`--api-url`

Base URL of Scylla Manager server (envvar SCYLLA_MANAGER_API_URL).

If running sctool on the same machine as server, it’s generated based on ‘/etc/scylla-manager/scylla-manager.yaml’ file.

**Default value:** `http://127.0.0.1:5080/api/v1`

## backup files

<!-- -*- mode: rst -*- -->

This command lists files that were uploaded during backup procedure.
It outputs the remote paths of files together with keyspace/table information separated by delimiter that you provide.

### Syntax

```none
sctool backup files --cluster <id|name> --snapshot-tag <tag> [flags]
```

### Command options

#### `--all-clusters`

Shows backups for all clusters.

**Default value:** `false`

#### `-c, --cluster`

The target cluster name or ID (envvar SCYLLA_MANAGER_CLUSTER).

#### `-d, --delimiter`

Dictates which character will be used as a whitespace between remote file path and information about keyspace and table.

**Default value:** \`\`	\`\`

#### `-h, --help`

help for files

**Default value:** `false`

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

#### `-L, --location`

A list of backup locations separated by a comma, specifies where to place the backup, the format is [<dc>:]<provider>:<bucket>.
The ‘<dc>’ parameter is optional it allows to specify location for a datacenter in a multi-dc setting, it must match Scylla nodes datacenter.
The supported storage ‘<provider>’s are ‘azure’, ‘gcs’, ‘s3’, ‘localstorage’.
The ‘bucket’ parameter is a bucket name, it must be an alphanumeric string and **may contain a dash and or a dot, but other characters are forbidden**.

**Default value:** `[]`

#### `-T, --snapshot-tag`

Snapshot tag as read from the backup listing.

#### `--with-version`

Renders table names with version UUID.

**Default value:** `false`

`--api-cert-file`

File path to HTTPS client certificate used to access the Scylla Manager server when client certificate validation is enabled (envvar SCYLLA_MANAGER_API_CERT_FILE).

`--api-key-file`

File path to HTTPS client key associated with –api-cert-file flag (envvar SCYLLA_MANAGER_API_KEY_FILE).

`--api-url`

Base URL of Scylla Manager server (envvar SCYLLA_MANAGER_API_URL).

If running sctool on the same machine as server, it’s generated based on ‘/etc/scylla-manager/scylla-manager.yaml’ file.

**Default value:** `http://127.0.0.1:5080/api/v1`
