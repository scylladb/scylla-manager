<a id="install-manager"></a>

# Install ScyllaDB Manager

> * [System requirements](#system-requirements)
> * [Install package](#install-package)
> * [Configure storage](#configure-storage)
>   * [Local node](#local-node)
>   * [Remote cluster](#remote-cluster)
> * [Run the scyllamgr_setup script](#run-the-scyllamgr-setup-script)
> * [Enable bash script completion](#enable-bash-script-completion)
> * [Start ScyllaDB Manager service](#start-scylladb-manager-service)
> * [Next steps](#next-steps)

#### NOTE
If you need to upgrade ScyllaDB Manager from a previous version please see the [Upgrade guide](https://manager.docs.scylladb.com/master/upgrade/index.md).

## System requirements

While a minimal server can run on a system with 2 cores and 1GB RAM, the following configuration is recommended:

* **CPU** - 2vCPUs
* **Memory** - 8GB+ DRAM

#### NOTE
If you are running [ScyllaDB Monitoring Stack](https://monitoring.docs.scylladb.com/stable/index.html) on the same server as ScyllaDB Manager, your system should also meet the minimal [Monitoring requirements](https://monitoring.docs.scylladb.com/stable/install/monitoring-stack.html#minimal-production-system-recommendations).

## Install package

Best practice is to install ScyllaDB Manager Server on a dedicated machine, not on a ScyllaDB production node.

Debian/Ubuntu

1. Import the ScyllaDB GPG signing key.
   > ```console
   > sudo mkdir -p /etc/apt/keyrings
   > sudo gpg --homedir /tmp --no-default-keyring --keyring /etc/apt/keyrings/scylladb.gpg --keyserver hkp://keyserver.ubuntu.com:80 --recv-keys a43e06657bac99e3
   > ```
2. Add the ScyllaDB Manager APT repository to your system.
   > ```console
   > sudo curl -o /etc/apt/sources.list.d/scylla-manager.list -L https://downloads.scylladb.com/deb/debian/scylladb-manager-3.12.list
   > ```
3. Install ScyllaDB Manager packages.
   > ```console
   > sudo apt update
   > sudo apt install -y scylla-manager-server
   > sudo apt install -y scylla-manager-client
   > ```

Centos/RHEL

1. Add the ScyllaDB Manager RPM repository to your system.
   > ```console
   > sudo curl -o /etc/yum.repos.d/scylla-manager.repo -L https://downloads.scylladb.com/rpm/centos/scylladb-manager-3.12.repo
   > ```
2. Install ScyllaDB Manager packages.
   > ```console
   > sudo yum install scylla-manager-server
   > sudo yum install scylla-manager-client
   > ```

## Configure storage

ScyllaDB Manager uses ScyllaDB to store its data.
You can either use a local one-node ScyllaDB cluster (recommended) or connect ScyllaDB Manager to a remote cluster.

### Local node

On the same node as you are installing ScyllaDB Manager, download and install ScyllaDB Server package.
There is no need to run the ScyllaDB setup, it is taken care of later, by the `scyllamgr_setup` script.
When it’s installed you can jump to [Run the scyllamgr_setup script]() section.

### Remote cluster

ScyllaDB Manager configuration file `/etc/scylla-manager/scylla-manager.yaml` contains a database configuration section.

```yaml
# ScyllaDB Manager database, used to store management data.
database:
  hosts:
    - 127.0.0.1
# Override default 9042 CQL port
#  port: 5555
#
# Enable or disable client/server encryption.
#  ssl: false
#
# Database credentials.
#  user: user
#  password: password
#
# Local datacenter name, specify if using a remote, multi-dc cluster.
#  local_dc:
#
# Database connection timeout.
#  timeout: 600ms
#
# Keyspace for management data, for create statement see /etc/scylla-manager/create_keyspace.cql.tpl.
#  keyspace: scylla_manager
```

Using an editor open the file and change relevant parameters.

**Procedure**

1. Edit the `hosts` parameter, change the IP address to the IP address or addressees of the remote cluster.
2. If authentication is needed, uncomment and edit the `user` and `password` parameters.
3. If it’s a single DC cluster, ScyllaDB Manager keyspace using NetworkTopologyStrategy would be created.
   Refer to [ScyllaDB Architecture - Fault Tolerance](https://docs.scylladb.com/architecture/architecture-fault-tolerance/) for more information on replication.
   It would have replication factor equal to the amount of racks in each data center. In case where this setup could lead to reduced availability
   (total replication factor smaller than 3), replication factor will be increased up to 3 (depending on node count) regardless of the rack count.
4. If it’s a multi DC cluster, create a keyspace named `scylla_manager` yourself.
   You can use a different keyspace name, just remember to adjust the `keyspace` parameter value.
   Set `local_dc` parameter to DC the closest to ScyllaDB Manager Server.
5. If client/server encryption is enabled, uncomment and set the `ssl` parameter to `true`.
   Additional SSL configuration options can be set in the `ssl` configuration section.

```yaml
# Optional custom client/server encryption options.
#ssl:
# CA certificate used to validate server cert. If not set will use host's root CA set.
#  cert_file:
#
# Verify the hostname and server cert.
#  validate: true
#
# Client certificate and key in PEM format. It has to be provided when
# client_encryption_options.require_client_auth=true is set on server.
#  user_cert_file:
#  user_key_file
```

Sample configuration of ScyllaDB Manager working with a remote cluster with authentication and replication factor 3 could look like this.

```yaml
database:
  hosts:
    - 198.100.51.11
    - 198.100.51.12
  user: user
  password: password
```

Sample mTLS configuration on the ScyllaDB Manager side (`scylla-manager.yaml`):

```yaml
database:
  hosts:
    - 198.100.51.11
  port: 9142
  ssl: true

ssl:
  validate: true
  cert_file: /etc/scylla-manager/certs/ca.crt
  user_cert_file: /etc/scylla-manager/certs/cl.crt
  user_key_file: /etc/scylla-manager/certs/cl.key
```

Sample mTLS [configuration](https://docs.scylladb.com/manual/stable/reference/configuration-parameters.html) on the ScyllaDB side (`scylla.yaml`):

```yaml
native_transport_port_ssl: 9142

client_encryption_options:
  enabled: true
  require_client_auth: true
  certificate: /etc/scylla/certs/db.crt
  keyfile: /etc/scylla/certs/db.key
  truststore: /etc/scylla/certs/ca.crt
```

Other TLS variants can be configured by disabling options like `ssl.validate` in `scylla-manager.yaml` or `client_encryption_options.require_client_auth` in `scylla.yaml`.

## Run the scyllamgr_setup script

The ScyllaDB Manager setup script automates configuration of ScyllaDB Manager by asking you some questions.
It can be run in non-interactive mode by using flags.

```none
scyllamgr_setup -h
Usage: scyllamgr_setup [-y][--no-scylla-setup][--no-enable-service][--no-check-for-updates]

Options:
  -y, --assume-yes          assume that the answer to any question which would be asked is yes
  --no-scylla-setup         skip setting up and enabling local Scylla instance as a storage backend for ScyllaDB Manager
  --no-enable-service       skip enabling service
  --no-check-for-updates    skip enabling periodic check for updates
  -h, --help                print this help

Interactive mode is enabled when no flags are provided.
```

Run the `scyllamgr_setup` script to configure the service.

## Enable bash script completion

Enable bash completion for sctool (the ScyllaDB Manager CLI) in the current bash session.
Alternatively, you can just open a new terminal.

```none
source /etc/bash_completion.d/sctool.bash
```

## Start ScyllaDB Manager service

ScyllaDB Manager integrates with `systemd` and can be started and stopped using `systemctl` command.

**Procedure**

1. Start the ScyllaDB Manager server service.
   ```none
   sudo systemctl start scylla-manager.service
   ```
2. Verify the ScyllaDB Manager server service is running.
   ```none
   sudo systemctl status scylla-manager.service -l
   ● scylla-manager.service - ScyllaDB Manager Server
      Loaded: loaded (/usr/lib/systemd/system/scylla-manager.service; enabled; vendor preset: disabled)
      Active: active (running) since Wed 2019-10-30 11:00:01 UTC; 20s ago
    Main PID: 5805 (scylla-manager)
      CGroup: /system.slice/scylla-manager.service
              └─5805 /usr/bin/scylla-manager
   ```

   #### NOTE
   The first time you start ScyllaDB Manager it may take a while. It creates a database schema. Follow the logs to see the progress.
3. Confirm sctool is running by displaying the sctool version.
   ```none
   sctool version
   Client version: 2.1-0.20200401.ce91f2ad
   Server version: 2.1-0.20200401.ce91f2ad
   ```

## Next steps

* [Install ScyllaDB Manager Agent](https://manager.docs.scylladb.com/master/install-scylla-manager-agent.md)
