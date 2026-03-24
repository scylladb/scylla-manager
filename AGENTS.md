# AGENTS.md — Scylla Manager

Scylla Manager (SM) is a management tool for Scylla clusters.
Scylla cluster is a distributed database serving traffic via either CQL or Alternator frontend.
The most important role of SM is to orchestrate repair, backup, restore tasks on managed Scylla cluster.
SM consists of three binaries: `scylla-manager` (SM server), `scylla-manager-agent` (per-node SM agent), and `sctool` (CLI for interacting with SM server).

## Project structure

It consists of multiple go modules:
- the main module (.) containing SM server and SM agent
- backupspec module (./backupspec) containing backup directory specification
- managerclient module (./v3/pkg/managerclient) containing client for SM server
- swagger module (./v3/swagger) containing definitions of used Scylla, SM server, SM agent swagger endpoints
- util module (./v3/pkg/util) containing generic legacy helpers (new helpers are added to ./pkg/util2 pkg in main module)

In case changes need to be made across multiple modules, they need to be merged in separate per module PRs.
Modules in go.mod can only reference commits from master branch.
All dependencies for main module (also other go modules from this repo) are vendored in /vendor dir.
Documentation is stored in ./docs/source dir.

### SM server architecture

SM server is built around its scheduler responsible for scheduling and triggering tasks and services responsible
for executing those tasks.
Scheduler is implemented on two levels:
- ./pkg/scheduler - implementation responsible for scheduling tasks at the right time
- ./pkg/service/scheduler - implementation responsible for triggering tasks with all required information

Services are implemented in ./pkg/service and the most important ones are:
- ./pkg/service/backup - responsible for backup task
- ./pkg/service/restore - responsible for restore task
- ./pkg/service/repair - responsible for repair task

The `sctool` CLI is implemented in ./pkg/command.

A single SM server can manage multiple Scylla clusters.
SM server stores cluster info, task definition and progress in a separate Scylla cluster (SM DB).
It's usually local, single node cluster. SM server communicates with SM DB over CQL.
SM server uses this information to start, pause, resume tasks and display their progress.

User interacts with SM server via HTTP/S REST API defined in swagger module.
To do so, user can use the `sctool` CLI or `managerclient` SDK.

SM server communicates with managed Scylla clusters mainly via SM agent proxy. This communication is always required.
In many other, but not all cases, SM server communicates with Scylla cluster directly over CQL.
It requires adding cluster with specified CQL credentials. The same goes for Scylla clusters using Alternator frontend.

### SM agent architecture

SM agent is a small server running on every Scylla node. It serves as a proxy for SM server to Scylla HTTP REST API,
which is exposed only on localhost. Additionally, SM agent also encapsulates rclone server. It is used for managing
backup files on Scylla nodes and in backup locations (e.g., S3, GS, Azure, Minio, etc.).
Communication with SM agent requires special auth token to be present in each request header.

SM agent is implemented in ./pkg/cmd/agent and its rclone component is implemented in ./pkg/rclone.

## General code guidelines

When executing tasks, SM server does not perform any CPU, disk, network intensive operations.
It serves as an orchestrator while Scylla node is doing all the heavy work. In case of backup
and restore, SM agent is also responsible for moving files from backup location, which is network
intensive operation.
The only resource that might be limited to SM server is its memory.
The rule of thumb is that SM server can keep all single node or table scope context needed for
running given task (e.g., entire SM backup manifest with all files listed in it).
Extra caution should be taken when storing entire cluster scope context in memory.
In such cases, it's preferable to change the implementation to a per node or table context,
unless this would result in less optimized usage of intensive Scylla node or SM agent API calls.

SM project is in maintenance state - meaning that no new SM features are added,
but only the changes needed to support new Scylla features are implemented.
SM is also supposed to support all currently supported Scylla versions.

Because of that, when working on SM server codebase, it's important to remember that:
- changes to existing codebase should be minimal and backward compatible
- new code should live mostly in new files and new packages
- new code should optimize the usage of intensive Scylla node and SM agent API calls
- new code should prioritize simplicity and readability over optimizing the usage of lightweight Scylla node and SM agent API calls
- new code shouldn't optimize SM codebase itself
- new code should take into consideration SM server memory consumption

If new code introduces user facing changes, it should also adjust the documentation.

## Build Commands

```bash
make build # Build all three binaries
```

## Test Commands

```bash
# Unit tests (all packages)
make unit-test

# Integration tests (per pkg or specific test, require already set up test dev env)
make pkg-integration-test PKG=./pkg/service/backup
make pkg-integration-test PKG=./pkg/service/repair RUN=TestName
```

## Lint and Format

```bash
make check # Full static analysis suite (should pass for every commit)
```

## Development Environment

```bash
make start-dev-env SCYLLA_VERSION=<version> TABLETS=<enabled|disabled> SSL_ENABLED=<true|false> # Start test dev env (6-node Scylla cluster, another 2-node cluster MinIO, etc.)
make run-server SSL_ENABLED=<true|false>                                                        # Build and run SM server in test dev env
```

## Testing

Code should be written with both unit tests and integration tests in mind.
Integration tests should follow table-driven approach whenever possible,
have separate setup and test execution stages and focus on comprehensiveness.
For the changes to be approved, the following checks must pass:
- make check
- make unit-test
- make pkg-integration-test PKG=<service path> RUN=<new test name>

## Commit Messages

Changes should be split into commits following Conventional Commits specification.
