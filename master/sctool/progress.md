# Progress

<a id="task-progress"></a>

## progress

<!-- -*- mode: rst -*- -->

This command shows details of the latest run (or still running) task.
If there is one task of the given type the ‘<id|name>’ argument is not needed.
‘Progress: X%’ means that X% of the task has been completed without any failures.
‘Progress: X%/Y%’ means that X% of the task has succeeded and Y% of the task has failed.

### Syntax

```none
sctool progress --cluster <id|name> [--details] [--run UUID] [flags] <type>[/<id|name>]
```

### Command options

#### `-c, --cluster`

The target cluster name or ID (envvar SCYLLA_MANAGER_CLUSTER).

#### `--details`

More detailed progress data, depending on task type.

**Default value:** `false`

#### `-h, --help`

help for progress

**Default value:** `false`

#### `--host`

A list of host glob patterns, e.g. ‘1.1.1.\*,!1.2.\*.4.’.
The following syntax for glob patterns is supported:

* ‘\*’ - matches any number of any characters including none
* ‘?’ - matches any single character
* ‘[abc]’ - matches one character given in the bracket
* ‘[a-z]’ - matches one character from the range given in the bracket

Patterns are evaluated from left to right.
If a pattern starts with ‘!’ it unselects items that were selected by previous patterns i.e. ‘a?,!aa’ selects *ab* but not *aa*.

**Default value:** `[]`

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

#### `--run`

Show progress of a particular run, see sctool info to get the IDs.

**Default value:** `latest`

`--api-cert-file`

File path to HTTPS client certificate used to access the Scylla Manager server when client certificate validation is enabled (envvar SCYLLA_MANAGER_API_CERT_FILE).

`--api-key-file`

File path to HTTPS client key associated with –api-cert-file flag (envvar SCYLLA_MANAGER_API_KEY_FILE).

`--api-url`

Base URL of Scylla Manager server (envvar SCYLLA_MANAGER_API_URL).

If running sctool on the same machine as server, it’s generated based on ‘/etc/scylla-manager/scylla-manager.yaml’ file.

**Default value:** `http://127.0.0.1:5080/api/v1`

### Example

```default
Get progress of latest repair task of cluster 'prod'.
sctool progress -c prod repair
```
