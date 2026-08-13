# Stop

<a id="task-stop"></a>

## stop

<!-- -*- mode: rst -*- -->

This command stops a specified task, stopping an already stopped task has no effect.
If you want to stop all tasks see sctool suspend.
If there is one task of the given type the ‘<id|name>’ argument is not needed.

### Syntax

```none
sctool stop --cluster <id|name> [flags] <type>[/<id|name>]
```

### Command options

#### `-c, --cluster`

The target cluster name or ID (envvar SCYLLA_MANAGER_CLUSTER).

#### `--delete`

Permanently remove the task.

**Default value:** `false`

#### `--disable`

Prevents the task from running until it has been re-enabled with the ‘sctool <task_type> update –enabled’ command.

**Default value:** `false`

#### `-h, --help`

help for stop

**Default value:** `false`

`--api-cert-file`

File path to HTTPS client certificate used to access the Scylla Manager server when client certificate validation is enabled (envvar SCYLLA_MANAGER_API_CERT_FILE).

`--api-key-file`

File path to HTTPS client key associated with –api-cert-file flag (envvar SCYLLA_MANAGER_API_KEY_FILE).

`--api-url`

Base URL of Scylla Manager server (envvar SCYLLA_MANAGER_API_URL).

If running sctool on the same machine as server, it’s generated based on ‘/etc/scylla-manager/scylla-manager.yaml’ file.

**Default value:** `http://127.0.0.1:5080/api/v1`
