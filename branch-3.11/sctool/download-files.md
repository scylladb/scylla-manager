# Download files

ScyllaDB Manager Agent comes with a download-files subcommand that given a backup location can be used to:

1. List clusters, datacenters and nodes.
2. Search for snapshot tags.
3. Download files without a need for third-party tools.

Please note, that the command may require higher demand on the number of file descriptors.
We recommend prefixing this command with ulimit to extend the default fd soft limit.

```none
ulimit -n 10000
```

## Download-files

**Syntax:**

```none
scylla-manager-agent download-files --location <backup location> [OPTION]...
```

<a id="download-files-parameters"></a>

### parameters

<a id="download-files-param-clear-tables"></a>

#### `--clear-tables`

Remove sstables before downloading

---

<a id="download-files-param-config-file"></a>

#### `-c, --config-file path`

Configuration file path, you can specify the flag multiple times to overwrite configuration options.

**Default:** /etc/scylla-manager-agent/scylla-manager-agent.yaml

---

<a id="download-files-param-data-dir"></a>

#### `-d, --data-dir path`

Path to ScyllaDB data directory (typically /var/lib/scylla/data) or other directory to use for downloading the files.

**Default:** current directory

---

<a id="download-files-param-debug"></a>

#### `--debug`

Enable debug logs.

**Default:** current directory

---

<a id="download-files-param-dry-run"></a>

#### `--dry-run`

Validate and print a plan without downloading (or clearing) any files.

---

<a id="download-files-param-dump-manifest"></a>

#### `--dump-manifest`

Print ScyllaDB Manager backup manifest as JSON.

---

<a id="download-files-param-dump-tokens"></a>

#### `--dump-tokens`

Print list of tokens from the manifest.

---

<a id="download-files-param-keyspace"></a>

#### `-K, --keyspace list`

A comma-separated list of keyspace/tables glob patterns, e.g. ‘keyspace,!keyspace.table_prefix_\*’.

---

<a id="download-files-param-list-nodes"></a>

#### `--list-nodes`

Print list of nodes including cluster name and node IP, this command would help you find nodes you can restore data from.

---

<a id="download-files-param-list-snapshots"></a>

#### `--list-snapshots`

Print list of snapshots of the specified node, this also takes into account keyspace filter and returns only snapshots containing any of requested keyspaces or tables, newest snapshots are printed first.

---

<a id="download-files-param-location"></a>

#### `-L, --location string`

Backup location in the format <provider>:<name> e.g. s3:my-bucket, the supported providers are: s3, gcs, azure.

---

<a id="download-files-param-mode"></a>

#### `--mode upload, sstableloader`

Mode changes resulting directory structure, supported values are: upload, sstableloader, set ‘upload’ to use table upload directories, set ‘sstableloader’ for <keyspace>/<table> directories layout.

---

<a id="download-files-param-node"></a>

#### `-n, --node ID`

‘Host ID’ value from nodetool status command output of a node you want to restore.

**Default:** local node

---

<a id="download-files-param-parallel"></a>

#### `-p, --parallel int`

How many files to download in parallel.

**Default:** 8

---

<a id="download-files-param-rate-limit"></a>

#### `--rate-limit int`

Rate limit in megabytes (MiB) per second.

**Default:** no limit

---

<a id="download-files-param-snapshot-tag"></a>

#### `-T, --snapshot-tag tag`

ScyllaDB Manager snapshot tag as read from backup listing e.g. sm_20060102150405UTC, use –list-snapshots to get a list of snapshots of the node.
