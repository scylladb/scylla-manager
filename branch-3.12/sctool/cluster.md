# Cluster

The cluster commands allow you to add, delete, list, and update clusters.
A ScyllaDB cluster must be added ([cluster add](#cluster-add)) before management tasks can be initiated.

<a id="cluster-add"></a>

## cluster add

<!-- -*- mode: rst -*- -->

This command adds the specified cluster to the Scylla Manager.
Once a Scylla cluster is added, a weekly repair task is also added.
Before continuing make sure the cluster that you want to add is prepared for it,
see [https://manager.docs.scylladb.com/stable/add-a-cluster.html](https://manager.docs.scylladb.com/stable/add-a-cluster.html) for instructions.

### Syntax

```none
sctool cluster add --host <IP> [--name <alias>] [--auth-token <token>] [flags]
```

### Command options

#### `--alternator-access-key-id`

Alternator access-key-id, for security reasons this account should NOT have access to your data.
If you specify the alternator access-key-id and secret-access-key, the alternator health check you see in status command would try to login and execute a query against system table.
Otherwise alternator health check is based on sending GET request to alternator port and does not create an alternator client.

#### `--alternator-secret-access-key`

Alternator secret-access-key associated with access-key-id.

#### `--auth-token`

The authentication token you identified in ‘/etc/scylla-manager-agent/scylla-manager-agent.yaml’.

#### `--force-non-ssl-session-port`

Forces Scylla Manager to always use the non-SSL port for TLS-enabled cluster CQL sessions.

**Default value:** `false`

#### `--force-tls-disabled`

Forces Scylla Manager to always disable TLS for the cluster’s CQL session, even if TLS is enabled in scylla.yaml.

**Default value:** `false`

#### `-h, --help`

help for add

**Default value:** `false`

#### `--host`

Hostname or IP of the node that will be used to discover other nodes belonging to the cluster.
Note that this will be persisted and used every time Scylla Manager starts.
You can use either an IPv4 or IPv6 address.

#### `-i, --id`

Explicitly specify cluster ID, when not provided random UUID will be generated.

#### `--label`

A comma-separated list of label modifications. Labels are represented as a key-value store.
Character ‘=’ has a special meaning and cannot be a part of label’s key nor value.
A single modification takes form of:
\* ‘<key>=<value>’ - sets the label <key> to <value>
\* ‘<key>-’        - removes the label

For example, ‘–label k1=v1,k2-’ will set the label ‘k1’ to ‘v1’ and will also remove label ‘k2’.

#### `-n, --name`

When a cluster is added, it is assigned a unique identifier.
Use this parameter to identify the cluster by an alias name which is more meaningful.
This alias name can be used with all commands that accept –cluster parameter.

#### `-p, --password`

CQL password associated with username.

#### `--port`

Alternate Scylla Manager agent port.

**Default value:** `10001`

#### `--ssl-user-cert-file`

File path to client certificate when Scylla uses client/server encryption (require_client_auth enabled).

#### `--ssl-user-key-file`

File path to key associated with –ssl-user-cert-file flag.

#### `-u, --username`

CQL username, for security reasons this user should NOT have access to your data.
If you specify the CQL username and password, the CQL health check you see in status command would try to login and execute a query against system keyspace.
Otherwise CQL health check is based on sending CQL OPTIONS frame and does not start a CQL session.

#### `--without-repair`

When cluster is added, Manager schedules repair to repeat weekly.
Use this flag to create a cluster without a scheduled repair.

**Default value:** `false`

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
sctool cluster add --host 34.203.122.52 --name prod-cluster --auth-token "6Es3dm24U72NzAu9ANWmU3C4ALyVZhwwPZZPWtK10eYGHJ24wMoh9SQxRZEluWMc0qDrsWCCshvfhk9uewOimQS2x5yNTYUEoIkO1VpSmTFu5fsFyoDgEkmNrCJpXtfM"
c1bbabf3-cad1-4a59-ab8f-84e2a73b623f
 __
/  \     Cluster added! You can set it as default, by exporting env variable.
@  @     $ export SCYLLA_MANAGER_CLUSTER=c1bbabf3-cad1-4a59-ab8f-84e2a73b623f
|  |     $ export SCYLLA_MANAGER_CLUSTER=prod-cluster
|| |/
|| ||    Now run:
|\_/|    $ sctool status -c prod-cluster
\___/    $ sctool tasks -c prod-cluster
```

<a id="cluster-delete"></a>

## cluster delete

<!-- -*- mode: rst -*- -->

This command deletes the specified cluster from the manager.
Note that there is no confirmation or warning to confirm.
If you deleted the cluster by mistake, you will need to add it again.
If you are removing the cluster from Scylla Manager and you are using Scylla Monitoring, remove the target from Prometheus Target list in the prometheus/scylla_manager_servers.yml file.

### Syntax

```none
sctool cluster delete --cluster <id|name> [flags]
```

### Command options

#### `-c, --cluster`

The target cluster name or ID (envvar SCYLLA_MANAGER_CLUSTER).

#### `-h, --help`

help for delete

**Default value:** `false`

`--api-cert-file`

File path to HTTPS client certificate used to access the Scylla Manager server when client certificate validation is enabled (envvar SCYLLA_MANAGER_API_CERT_FILE).

`--api-key-file`

File path to HTTPS client key associated with –api-cert-file flag (envvar SCYLLA_MANAGER_API_KEY_FILE).

`--api-url`

Base URL of Scylla Manager server (envvar SCYLLA_MANAGER_API_URL).

If running sctool on the same machine as server, it’s generated based on ‘/etc/scylla-manager/scylla-manager.yaml’ file.

**Default value:** `http://127.0.0.1:5080/api/v1`

<a id="cluster-list"></a>

## cluster list

<!-- -*- mode: rst -*- -->

This command displays a list of managed clusters.

### Syntax

```none
sctool cluster list [flags]
```

### Command options

#### `-h, --help`

help for list

**Default value:** `false`

`--api-cert-file`

File path to HTTPS client certificate used to access the Scylla Manager server when client certificate validation is enabled (envvar SCYLLA_MANAGER_API_CERT_FILE).

`--api-key-file`

File path to HTTPS client key associated with –api-cert-file flag (envvar SCYLLA_MANAGER_API_KEY_FILE).

`--api-url`

Base URL of Scylla Manager server (envvar SCYLLA_MANAGER_API_URL).

If running sctool on the same machine as server, it’s generated based on ‘/etc/scylla-manager/scylla-manager.yaml’ file.

**Default value:** `http://127.0.0.1:5080/api/v1`

<a id="cluster-update"></a>

## cluster update

<!-- -*- mode: rst -*- -->

This command modifies managed cluster parameters

### Syntax

```none
sctool cluster update --cluster <id|name> [flags]
```

### Command options

#### `--alternator-access-key-id`

Alternator access-key-id, for security reasons this account should NOT have access to your data.
If you specify the alternator access-key-id and secret-access-key, the alternator health check you see in status command would try to login and execute a query against system table.
Otherwise alternator health check is based on sending GET request to alternator port and does not create an alternator client.

#### `--alternator-secret-access-key`

Alternator secret-access-key associated with access-key-id.

#### `--auth-token`

The authentication token you identified in ‘/etc/scylla-manager-agent/scylla-manager-agent.yaml’.

#### `-c, --cluster`

The target cluster name or ID (envvar SCYLLA_MANAGER_CLUSTER).

#### `--delete-alternator-credentials`

Deletes alternator access-key-id and secret-access-key, added with –alternator-access-key-id and –alternator-secret-access-key.

**Default value:** `false`

#### `--delete-cql-credentials`

Deletes CQL username and password, added with –username and –password.

**Default value:** `false`

#### `--delete-ssl-user-cert`

Deletes SSL user certificate, added with –ssl-user-cert-file flag.

**Default value:** `false`

#### `--force-non-ssl-session-port`

Forces Scylla Manager to always use the non-SSL port for TLS-enabled cluster CQL sessions.

**Default value:** `false`

#### `--force-tls-disabled`

Forces Scylla Manager to always disable TLS for the cluster’s CQL session, even if TLS is enabled in scylla.yaml.

**Default value:** `false`

#### `-h, --help`

help for update

**Default value:** `false`

#### `--host`

Hostname or IP of the node that will be used to discover other nodes belonging to the cluster.
Note that this will be persisted and used every time Scylla Manager starts.
You can use either an IPv4 or IPv6 address.

#### `--label`

A comma-separated list of label modifications. Labels are represented as a key-value store.
Character ‘=’ has a special meaning and cannot be a part of label’s key nor value.
A single modification takes form of:
\* ‘<key>=<value>’ - sets the label <key> to <value>
\* ‘<key>-’        - removes the label

For example, ‘–label k1=v1,k2-’ will set the label ‘k1’ to ‘v1’ and will also remove label ‘k2’.

#### `-n, --name`

When a cluster is added, it is assigned a unique identifier.
Use this parameter to identify the cluster by an alias name which is more meaningful.
This alias name can be used with all commands that accept –cluster parameter.

#### `-p, --password`

CQL password associated with username.

#### `--port`

Alternate Scylla Manager agent port.

**Default value:** `10001`

#### `--ssl-user-cert-file`

File path to client certificate when Scylla uses client/server encryption (require_client_auth enabled).

#### `--ssl-user-key-file`

File path to key associated with –ssl-user-cert-file flag.

#### `-u, --username`

CQL username, for security reasons this user should NOT have access to your data.
If you specify the CQL username and password, the CQL health check you see in status command would try to login and execute a query against system keyspace.
Otherwise CQL health check is based on sending CQL OPTIONS frame and does not start a CQL session.

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
In this example, the cluster named ``cluster`` has been renamed to ``prod-cluster``.

sctool cluster update -c cluster --name prod-cluster
```
