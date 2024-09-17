![GitHub release](https://img.shields.io/github/tag/scylladb/scylla-manager.svg?label=release)
![Sanity check](https://github.com/scylladb/scylla-manager/actions/workflows/sanity-checks.yml/badge.svg?branch=master)
![Language](https://img.shields.io/badge/Language-Go-blue.svg)

# Scylla Manager

Welcome to Scylla Manager repository!
Scylla Manager user docs can be found [here](https://manager.docs.scylladb.com/stable/).

Scylla Manager consists of tree components:

1. a server that manages Scylla clusters
2. an agent running on each node
3. `sctool` - a CLI interface to the server

## Scylla integration status

| ScyllaDB version      | Workflows                                                                                                                                 | Limitations                                                                                                                                           |
|-----------------------|-------------------------------------------------------------------------------------------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------|
| **2024.1.9**          | ![integration-tests-2024.1.9-IPV4]<br/>![integration-tests-2024.1.9-IPV6]                                                                 | Restoration of schema into cluster with `consistant_cluster_management: true` is not supported                                                        |
| **2023.1.11**         | ![integration-tests-2023.1.11-IPV4]<br/>![integration-tests-2023.1.11-IPV4-raftschema]<br/>![integration-tests-2023.1.11-IPV6-raftschema] | Restoration of schema into cluster with `consistant_cluster_management: true` is not supported                                                        |
| **6.1.1**             | ![integration-tests-6.1.1-IPV4]<br/>![integration-tests-6.1.1-IPV4-tablets]<br/>![integration-tests-6.1.1-IPV6-tablets]                   | Restoration of **Authentication** and **Service Levels** is not supported<br/>Restoration of schema containing **Alternator** tables is not supported |
| **latest-enterprise** | ![integration-tests-latest-enterprise-IPV4]<br/>![integration-tests-latest-enterprise-IPV4-tablets]                                       | Restoration of **Authentication** and **Service Levels** is not supported<br/>Restoration of schema containing **Alternator** tables is not supported |

[integration-tests-2024.1.9-IPV4]: https://github.com/scylladb/scylla-manager/actions/workflows/integration-tests-2024.1.9-IPV4.yaml/badge.svg?branch=master
[integration-tests-2024.1.9-IPV6]: https://github.com/scylladb/scylla-manager/actions/workflows/integration-tests-2024.1.9-IPV6.yaml/badge.svg?branch=master
[integration-tests-2023.1.11-IPV4]: https://github.com/scylladb/scylla-manager/actions/workflows/integration-tests-2023.1.11-IPV4.yaml/badge.svg?branch=master
[integration-tests-2023.1.11-IPV4-raftschema]: https://github.com/scylladb/scylla-manager/actions/workflows/integration-tests-2023.1.11-IPV4-raftschema.yaml/badge.svg?branch=master
[integration-tests-2023.1.11-IPV6-raftschema]: https://github.com/scylladb/scylla-manager/actions/workflows/integration-tests-2023.1.11-IPV6-raftschema.yaml/badge.svg?branch=master
[integration-tests-6.1.1-IPV4]: https://github.com/scylladb/scylla-manager/actions/workflows/integration-tests-6.1.1-IPV4.yaml/badge.svg?branch=master
[integration-tests-6.1.1-IPV4-tablets]: https://github.com/scylladb/scylla-manager/actions/workflows/integration-tests-6.1.1-IPV4-tablets.yaml/badge.svg?branch=master
[integration-tests-6.1.1-IPV6-tablets]: https://github.com/scylladb/scylla-manager/actions/workflows/integration-tests-6.1.1-IPV6-tablets.yaml/badge.svg?branch=master
[integration-tests-latest-enterprise-IPV4]: https://github.com/scylladb/scylla-manager/actions/workflows/integration-tests-latest-enterprise-IPV4.yaml/badge.svg?branch=master
[integration-tests-latest-enterprise-IPV4-tablets]: https://github.com/scylladb/scylla-manager/actions/workflows/integration-tests-latest-enterprise-IPV4-tablets.yaml/badge.svg?branch=master

## Installing and updating Go

The desired Go version is specified in `.go-version` file.
You can install or update Go version by running `make install` in `go` directory.
It would install the required version alongside your current version assuming that go is extracted from an official tar package.
If you do not have any Go installed at this point you can pass TARGET variable to specify a directory where to install Go. 

**Procedure**:

1. Run `make -C go install TARGET=/path/to/go/sdks/goversion` example `make install TARGET=~/tools/go/go1.15.5.linux-amd64`
2. Define `GOROOT` environment variable as `<git-root>/go/latest`

## Installing other packages needed for development

1. Install Docker
2. Run installation script `./install-dependencies.sh`

## Configuring imports formatting in GoLand

If using GoLand update import grouping policy:

1. Open File -> Settings -> Editor -> Code Style -> Go
2. Go to Imports Pane
3. Set "Sorting type" to goimports
4. Check every checkbox but "Group current project imports"
5. Press OK

## Increase fs.aio-max-nr

In order to run a Scylla it's required to increase the number of async i/o operations limit. 

```
echo "fs.aio-max-nr = 1048576" > /etc/sysctl.d/50-scylla.conf
```

## Running a development environment

Let's start the development environment.

```bash
make start-dev-env
```

for IPv6 environment:
```bash
IPV6=true make start-dev-env
```

This command will:
1. Build custom Scylla Docker image (testing/scylla)
2. Compile server, agent and sctool binaries
3. Start Scylla cluster with 2 DCs 3 nodes each (6 containers)
4. Start MinIO and Prometheus containers
5. Start dedicated Scylla container for Scylla Manager datastore

Additionally, you can start container with scylla-manager server.
```bash
make run-server
```

Docker compose environment for test cluster is located in the `testing` directory.

Once `scylla-manager` is bootstrapped use `./sctool` to add information about the cluster to the manager:

```bash
./sctool cluster add --host=192.168.100.11 --auth-token token --name my-cluster 
```

You can ask Scylla Manager to give the status of the cluster:

```bash
./sctool status
```

For other commands consult [user manual](https://docs.scylladb.com/operating-scylla/manager/).

### Helpful Makefile targets

Make is self-documenting run `make` or `make help` to see available targets with descriptions.

More Makefile targets are available in `testing` directory.

## Running tests

If test environment is running correctly you can run tests with:

```bash
make test
```

This runs both unit and integration tests. You can run them separately with:

```bash
make unit-test
make integration-test
make pkg-integration-test PKG=pkg/service/foo
```

For IPv6 environment:
```bash
make unit-test
IPV6=true make integration-test
IPV6=true make pkg-integration-test PKG=pkg/service/foo
```

Project testing heavily depends on integration tests, which are slow.
Tests should run for a couple of minutes.
All tests should succeed.

## Extending HTTP clients

There are two HTTP APIs that are specified with Swagger format, the API of the Scylla node and the API of the `scylla-manager` itself.

Client implementations are separated into packages:

- `scyllaclient` which contains client for the Scylla API along with Swagger specification file `scylla-api.json` and,
- `managerclient` which contains client for the `scylla-manager` API along with Swagger specification file `scylla-manager-api.json`.

Both clients are generated automatically by shell scripts.
To refresh generated packages from Swagger specification run:

```bash
make generate

# Or for generating specific client package ex.
go generate ./managerclient
go generate ./scyllaclient
```

## Sending patches

Develop on dedicated branch rooted at master.
Pull master regularly and rebase your work upon master whenever your dev branch is behind.
You are allowed and required to force push to your branches to keep the history neat and clean.

Document your work in commit messages.
Explain why you made the changes and mention IDs of the affected issues on GitHub.

Before pushing please run `make check` and `make test` to make sure all tests pass.

When satisfied create a pull request onto master.
All pull requests have to go through review process before they are merged.

## License

Scylla Manager Source Code is available under [Scylla Source Available License (SSAL)](https://www.scylladb.com/scylla-source-available-license/).
