<a id="task-commands"></a>

# Tasks

<a id="task-list"></a>

## tasks

<!-- -*- mode: rst -*- -->

This command shows all of the scheduled tasks for the specified cluster.
If cluster is not set this would output a table for every cluster.
Each row contains task type and ID, separated by a slash, task properties, next activation and last status information.
For more information on a task consult history and progress.

### Syntax

```none
sctool tasks [flags]
```

### Command options

#### `-a, --all`

Lists all tasks, including those which have been disabled, disabled tasks are prefixed with ‘\*’.

**Default value:** `false`

#### `-c, --cluster`

The target cluster name or ID (envvar SCYLLA_MANAGER_CLUSTER).

#### `-h, --help`

help for tasks

**Default value:** `false`

#### `--show-ids`

Always display task UUID, do not show task names.

**Default value:** `false`

#### `--show-properties`

Additionally display task properties.
Only displays properties set to non-default values.

**Default value:** `false`

#### `--sort`

Returns a list of tasks sorted according to the last run status and sort key which you provide.
Accepted sort key values are: ‘next-activation’, ‘status’.

#### `-s, --status`

Filters tasks according to their last run status.
Accepted values are: NEW, RUNNING, STOPPING, STOPPED, WAITING, DONE, ERROR, ABORTED.

#### `-t, --type`

Display only tasks of a given type.

`--api-cert-file`

File path to HTTPS client certificate used to access the Scylla Manager server when client certificate validation is enabled (envvar SCYLLA_MANAGER_API_CERT_FILE).

`--api-key-file`

File path to HTTPS client key associated with –api-cert-file flag (envvar SCYLLA_MANAGER_API_KEY_FILE).

`--api-url`

Base URL of Scylla Manager server (envvar SCYLLA_MANAGER_API_URL).

If running sctool on the same machine as server, it’s generated based on ‘/etc/scylla-manager/scylla-manager.yaml’ file.

**Default value:** `http://127.0.0.1:5080/api/v1`
