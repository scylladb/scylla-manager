# Version

## version

<!-- -*- mode: rst -*- -->

This command shows the currently installed sctool version and the Scylla Manager server version.

### Syntax

```none
sctool version [flags]
```

### Command options

#### `-h, --help`

help for version

**Default value:** `false`

`--api-cert-file`

File path to HTTPS client certificate used to access the Scylla Manager server when client certificate validation is enabled (envvar SCYLLA_MANAGER_API_CERT_FILE).

`--api-key-file`

File path to HTTPS client key associated with –api-cert-file flag (envvar SCYLLA_MANAGER_API_KEY_FILE).

`--api-url`

Base URL of Scylla Manager server (envvar SCYLLA_MANAGER_API_URL).

If running sctool on the same machine as server, it’s generated based on ‘/etc/scylla-manager/scylla-manager.yaml’ file.

**Default value:** `http://127.0.0.1:5080/api/v1`

### Example: version

```none
sctool version
Client version: 2.1-0.20200401.ce91f2ad
Server version: 2.1-0.20200401.ce91f2ad
```
