# Suspend & Resume

The suspend command stops execution of **all** tasks that are running on a cluster.
The tasks can be resumed using the resume command.
If you want to stop a specific task, use the [task stop command](https://manager.docs.scylladb.com/master/sctool/stop.md#task-stop).

<a id="suspend"></a>

## suspend

<!-- -*- mode: rst -*- -->

This command stops execution of **all** tasks that are running on a cluster.
The tasks can be resumed using the resume command.

When the suspend command is executed:

* The running tasks are stopped
* The scheduled tasks are canceled
* Starting a task manually fails

The health check tasks are an exception and they run even after suspend.

### Syntax

```none
sctool suspend --cluster <id|name> [--duration] [--on-resume-start-tasks] [flags]
```

### Command options

#### `--allow-task-type`

Tasks of this type will keep on being executed and scheduled even when cluster is suspended.

#### `-c, --cluster`

The target cluster name or ID (envvar SCYLLA_MANAGER_CLUSTER).

#### `--cron`

Task schedule as a cron expression.
It supports the extended syntax including @monthly, @weekly, @daily, @midnight, @hourly, @every X[h|m|s].

#### `--duration`

Automatically resume after the given duration X[h|m|s].

#### `--enabled`

Not enabled tasks are not executed and are hidden from the task list.

**Default value:** `true`

#### `-h, --help`

help for suspend

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

#### `--no-continue`

Start tasks from scratch on resume. It allows for performing additional cleanup of disabled tasks.
Doesn’t work with –duration.

**Default value:** `false`

#### `-r, --num-retries`

Number of times a task reruns following a failure.

**Default value:** `3`

#### `--on-resume-start-tasks`

On resume start tasks that were stopped by the suspend.

**Default value:** `false`

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

#### `--suspend-policy`

Describes behavior towards running tasks (other than ‘–allow-task-type’).
Doesn’t work with –duration.
It accepts the following values:

* ‘stop_running_tasks’: results in stopping running tasks.
* ‘fail_if_running_tasks’: results in failing to suspend cluster and returning 409 HTTP status code.

**Default value:** `stop_running_tasks`

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

<a id="resume"></a>

## resume

<!-- -*- mode: rst -*- -->

This command reschedules the suspended tasks.

### Syntax

```none
sctool resume --cluster <id|name> [flags]
```

### Command options

#### `-c, --cluster`

The target cluster name or ID (envvar SCYLLA_MANAGER_CLUSTER).

#### `-h, --help`

help for resume

**Default value:** `false`

#### `--no-continue`

Start tasks from scratch.

**Default value:** `false`

#### `--start-tasks`

Starts tasks that were stopped by the suspend command.

**Default value:** `false`

#### `--start-tasks-missed-activation`

Start tasks which missed their activation during suspend.

**Default value:** `false`

`--api-cert-file`

File path to HTTPS client certificate used to access the Scylla Manager server when client certificate validation is enabled (envvar SCYLLA_MANAGER_API_CERT_FILE).

`--api-key-file`

File path to HTTPS client key associated with –api-cert-file flag (envvar SCYLLA_MANAGER_API_KEY_FILE).

`--api-url`

Base URL of Scylla Manager server (envvar SCYLLA_MANAGER_API_URL).

If running sctool on the same machine as server, it’s generated based on ‘/etc/scylla-manager/scylla-manager.yaml’ file.

**Default value:** `http://127.0.0.1:5080/api/v1`
