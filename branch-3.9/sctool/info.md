# Info

<a id="task-info"></a>

## info

<!-- -*- mode: rst -*- -->

This command shows a details about task run history for a given task.
If there is one task of the given type the ‘<id|name>’ argument is not needed.

### Syntax

```none
sctool info --cluster <id|name> [flags] <type>[/<id|name>]
```

### Command options

#### `--cause`

Prints the cause of a failed task.

**Default value:** `false`

#### `-c, --cluster`

The target cluster name or ID (envvar SCYLLA_MANAGER_CLUSTER).

#### `-h, --help`

help for info

**Default value:** `false`

#### `--limit`

Limits the number of returned results.

**Default value:** `10`

`--api-cert-file`

File path to HTTPS client certificate used to access the Scylla Manager server when client certificate validation is enabled (envvar SCYLLA_MANAGER_API_CERT_FILE).

`--api-key-file`

File path to HTTPS client key associated with –api-cert-file flag (envvar SCYLLA_MANAGER_API_KEY_FILE).

`--api-url`

Base URL of Scylla Manager server (envvar SCYLLA_MANAGER_API_URL).

If running sctool on the same machine as server, it’s generated based on ‘/etc/scylla-manager/scylla-manager.yaml’ file.

**Default value:** `http://127.0.0.1:5080/api/v1`
