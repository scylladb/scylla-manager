<a id="repair-commands"></a>

# Repair

The repair commands allow you to: create and update a repair (ad-hoc or scheduled), and change selected parameters while a repair is running.

<a id="sctool-repair"></a>

## repair

<!-- -*- mode: rst -*- -->

This command allows you to schedule or run ad-hoc cluster repair.
Repair speed is controlled by two flags –parallel and –intensity.
The values of those flags can be adjusted while a repair is running using the control subcommand.

### Syntax

```none
sctool repair --cluster <id|name> [--intensity] [--parallel] [flags]
```

### Command options

#### `--allow-empty`

By default, scheduling or running repair that would target no table ends with an error.
This flag allows to schedule and run such repairs without an error.

**Default value:** `false`

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

Validates and displays repair information without actually scheduling the repair.
This allows you to display what will happen should the repair run with the parameters you set.

**Default value:** `false`

#### `--enabled`

Not enabled tasks are not executed and are hidden from the task list.

**Default value:** `true`

#### `--fail-fast`

Stops the task run on the first error.

**Default value:** `false`

#### `-h, --help`

help for repair

**Default value:** `false`

#### `--host`

Address of a node to repair, you can use either an IPv4 or IPv6 address.
Specifying the host flag limits repair to token ranges replicated by a given node.
It can be used in conjunction with –dc flag, in such a case the node must belong to the specified datacenters.

#### `--ignore-down-hosts`

Do not repair nodes that are down i.e. in status DN.

**Default value:** `false`

#### `--incremental-mode`

Incremental repair mode used when repairing tablet tables.
Available since ScyllaDB 2025.4. Setting it for older versions has no effect.
It accepts the following values:

* ‘incremental’: The incremental repair logic is enabled. Unrepaired sstables will be included for repair. Repaired sstables will be skipped. The incremental repair states will be updated after repair.
* ‘full’: The incremental repair logic is enabled. Both repaired and unrepaired sstables will be included for repair. The incremental repair states will be updated after repair.
* ‘disabled’: The incremental repair logic is disabled completely. The incremental repair states, e.g., repaired_at in sstables and sstables_repaired_at in the system.tablets table, will not be updated after repair.

#### `--intensity`

How many token ranges to repair in a single Scylla node at the same time.
Zero (0) is a special value, the number of token ranges is adjusted to the maximum supported (see repair docs for more information).
Changing the intensity impacts repair granularity if you need to resume it, the higher the value the more work on resume.
If you set intensity to a value greater than the maximum supported by the node, intensity will be capped at that maximum.
See effectively used intensity value in the display of ‘sctool progress repair’ command.

**Default value:** `1`

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

**Default value:** `[*,!system_traces]`

#### `--keyspace-replication`

Repair keyspaces with specified replication only.
When used with other filtering flags (e.g. ‘–keyspace’), repaired keyspaces need to match all filtering criteria.
It accepts the following values:

* ‘all’: Repair all keyspaces.
* ‘tablets’: Repair keyspaces replicated with tablets only.
* ‘vnodes’: Repair keyspaces replicated with vnodes only. It also allows for running this task in parallel with the dedicated tablet repair task.

**Default value:** `all`

#### `--label`

A comma-separated list of label modifications. Labels are represented as a key-value store.
Character ‘=’ has a special meaning and cannot be a part of label’s key nor value.
A single modification takes form of:
\* ‘<key>=<value>’ - sets the label <key> to <value>
\* ‘<key>-’        - removes the label

For example, ‘–label k1=v1,k2-’ will set the label ‘k1’ to ‘v1’ and will also remove label ‘k2’.

#### `--name`

Task name that can be used instead of ID.

#### `-r, --num-retries`

Number of times a task reruns following a failure.

**Default value:** `3`

#### `--parallel`

The maximum number of Scylla repair jobs that can run at the same time (on different token ranges and replicas).
Each node can take part in at most one repair at any given moment. By default the maximum possible parallelism is used.
The maximal effective parallelism depends on keyspace replication strategy and cluster topology (see repair docs for more information).
If you set parallel to a value greater than the maximum supported by the node, parallel will be capped at that maximum.
See effectively used parallel value in the display of ‘sctool progress repair’ command.

**Default value:** `0`

#### `--retry-wait`

Initial exponential backoff duration X[h|m|s].
With –retry-wait 10m task will wait 10 minutes, 20 minutes and 40 minutes after first, second and third consecutire failure.

**Default value:** `10m`

#### `--show-tables`

Prints table names together with keyspace, used in combination with –dry-run.

**Default value:** `false`

#### `--small-table-threshold`

Enables small table optimization for tables of size lower than given threshold, supported units [B, M, G, T].

**Default value:** `1GiB`

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

<a id="repair-control"></a>

## repair control

<!-- -*- mode: rst -*- -->

This command allows you to modify repair parameters while a repair is running.
Note that modified parameters apply only to the currently running task and not to the future task runs.
Moreover, this change is applied only to newly created ScyllaDB repair jobs (and to the currently repaired table).
This means that all currently running jobs will continue to run with previous parallel/intensity settings,
but it shouldn’t take much time for them to finish and observe the new behavior.
For modifying future repair task runs see ‘sctool repair update’ command.

### Syntax

```none
sctool repair control --cluster <id|name> [flags]
```

### Command options

#### `-c, --cluster`

The target cluster name or ID (envvar SCYLLA_MANAGER_CLUSTER).

#### `-h, --help`

help for control

**Default value:** `false`

#### `--intensity`

How many token ranges to repair in a single Scylla node at the same time.
Zero (0) is a special value, the number of token ranges is adjusted to the maximum supported (see repair docs for more information).
Changing the intensity impacts repair granularity if you need to resume it, the higher the value the more work on resume.
If you set intensity to a value greater than the maximum supported by the node, intensity will be capped at that maximum.
See effectively used intensity value in the display of ‘sctool progress repair’ command.

**Default value:** `1`

#### `--parallel`

The maximum number of Scylla repair jobs that can run at the same time (on different token ranges and replicas).
Each node can take part in at most one repair at any given moment. By default the maximum possible parallelism is used.
The maximal effective parallelism depends on keyspace replication strategy and cluster topology (see repair docs for more information).
If you set parallel to a value greater than the maximum supported by the node, parallel will be capped at that maximum.
See effectively used parallel value in the display of ‘sctool progress repair’ command.

**Default value:** `0`

`--api-cert-file`

File path to HTTPS client certificate used to access the Scylla Manager server when client certificate validation is enabled (envvar SCYLLA_MANAGER_API_CERT_FILE).

`--api-key-file`

File path to HTTPS client key associated with –api-cert-file flag (envvar SCYLLA_MANAGER_API_KEY_FILE).

`--api-url`

Base URL of Scylla Manager server (envvar SCYLLA_MANAGER_API_URL).

If running sctool on the same machine as server, it’s generated based on ‘/etc/scylla-manager/scylla-manager.yaml’ file.

**Default value:** `http://127.0.0.1:5080/api/v1`

<a id="repair-update"></a>

<a id="reschedule-a-repair"></a>

## repair update

<!-- -*- mode: rst -*- -->

This command allows you to modify parameters of an already existing repair task.
If there is one repair task the ‘repair/task-id’ argument is not needed.
Note that modified parameters apply only to the future task runs and not to the already running task.
For modifying already running repair task on the fly see ‘sctool repair control’ command.

### Syntax

```none
sctool repair update --cluster <id|name> [flags] [<repair/task-id>]
```

### Command options

#### `--allow-empty`

By default, scheduling or running repair that would target no table ends with an error.
This flag allows to schedule and run such repairs without an error.

**Default value:** `false`

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

Validates and displays repair information without actually scheduling the repair.
This allows you to display what will happen should the repair run with the parameters you set.

**Default value:** `false`

#### `--enabled`

Not enabled tasks are not executed and are hidden from the task list.

**Default value:** `true`

#### `--fail-fast`

Stops the task run on the first error.

**Default value:** `false`

#### `-h, --help`

help for update

**Default value:** `false`

#### `--host`

Address of a node to repair, you can use either an IPv4 or IPv6 address.
Specifying the host flag limits repair to token ranges replicated by a given node.
It can be used in conjunction with –dc flag, in such a case the node must belong to the specified datacenters.

#### `--ignore-down-hosts`

Do not repair nodes that are down i.e. in status DN.

**Default value:** `false`

#### `--incremental-mode`

Incremental repair mode used when repairing tablet tables.
Available since ScyllaDB 2025.4. Setting it for older versions has no effect.
It accepts the following values:

* ‘incremental’: The incremental repair logic is enabled. Unrepaired sstables will be included for repair. Repaired sstables will be skipped. The incremental repair states will be updated after repair.
* ‘full’: The incremental repair logic is enabled. Both repaired and unrepaired sstables will be included for repair. The incremental repair states will be updated after repair.
* ‘disabled’: The incremental repair logic is disabled completely. The incremental repair states, e.g., repaired_at in sstables and sstables_repaired_at in the system.tablets table, will not be updated after repair.

#### `--intensity`

How many token ranges to repair in a single Scylla node at the same time.
Zero (0) is a special value, the number of token ranges is adjusted to the maximum supported (see repair docs for more information).
Changing the intensity impacts repair granularity if you need to resume it, the higher the value the more work on resume.
If you set intensity to a value greater than the maximum supported by the node, intensity will be capped at that maximum.
See effectively used intensity value in the display of ‘sctool progress repair’ command.

**Default value:** `1`

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

**Default value:** `[*,!system_traces]`

#### `--keyspace-replication`

Repair keyspaces with specified replication only.
When used with other filtering flags (e.g. ‘–keyspace’), repaired keyspaces need to match all filtering criteria.
It accepts the following values:

* ‘all’: Repair all keyspaces.
* ‘tablets’: Repair keyspaces replicated with tablets only.
* ‘vnodes’: Repair keyspaces replicated with vnodes only. It also allows for running this task in parallel with the dedicated tablet repair task.

**Default value:** `all`

#### `--label`

A comma-separated list of label modifications. Labels are represented as a key-value store.
Character ‘=’ has a special meaning and cannot be a part of label’s key nor value.
A single modification takes form of:
\* ‘<key>=<value>’ - sets the label <key> to <value>
\* ‘<key>-’        - removes the label

For example, ‘–label k1=v1,k2-’ will set the label ‘k1’ to ‘v1’ and will also remove label ‘k2’.

#### `--name`

Task name that can be used instead of ID.

#### `-r, --num-retries`

Number of times a task reruns following a failure.

**Default value:** `3`

#### `--parallel`

The maximum number of Scylla repair jobs that can run at the same time (on different token ranges and replicas).
Each node can take part in at most one repair at any given moment. By default the maximum possible parallelism is used.
The maximal effective parallelism depends on keyspace replication strategy and cluster topology (see repair docs for more information).
If you set parallel to a value greater than the maximum supported by the node, parallel will be capped at that maximum.
See effectively used parallel value in the display of ‘sctool progress repair’ command.

**Default value:** `0`

#### `--retry-wait`

Initial exponential backoff duration X[h|m|s].
With –retry-wait 10m task will wait 10 minutes, 20 minutes and 40 minutes after first, second and third consecutire failure.

**Default value:** `10m`

#### `--show-tables`

Prints table names together with keyspace, used in combination with –dry-run.

**Default value:** `false`

#### `--small-table-threshold`

Enables small table optimization for tables of size lower than given threshold, supported units [B, M, G, T].

**Default value:** `1GiB`

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

<a id="repair-tablet"></a>

## repair tablet

<!-- -*- mode: rst -*- -->

This command allows you to schedule or run ad-hoc cluster tablet repair.
It requires ScyllaDB version 2025.1 or higher.
This repair targets all tablet keyspaces.

### Syntax

```none
sctool repair tablet --cluster <id|name> [flags]
```

### Command options

#### `-c, --cluster`

The target cluster name or ID (envvar SCYLLA_MANAGER_CLUSTER).

#### `--cron`

Task schedule as a cron expression.
It supports the extended syntax including @monthly, @weekly, @daily, @midnight, @hourly, @every X[h|m|s].

#### `--enabled`

Not enabled tasks are not executed and are hidden from the task list.

**Default value:** `true`

#### `-h, --help`

help for tablet

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

#### `--name`

Task name that can be used instead of ID.

#### `-r, --num-retries`

Number of times a task reruns following a failure.

**Default value:** `3`

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
