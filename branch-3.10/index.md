# ScyllaDB Manager

![image](images/header.png)

## Slack Channel

If you have any troubles or questions regarding ScyllaDB Manager contact us on [ScyllaDB Manager Slack channel](https://scylladb-users.slack.com/archives/C01ERVBPWLU).

## Introduction

ScyllaDB Manager automates database operations.
With ScyllaDB Manager you can schedule tasks such as backups and repairs, check cluster status, and more.
ScyllaDB Manager can manage multiple ScyllaDB clusters and run cluster-wide tasks in a controlled and predictable way.

ScyllaDB Manager consists of three components:

* Server - a daemon that exposes a REST API
* sctool - a command-line interface (CLI) for interacting with the Server
* Agent - a daemon, installed on each ScyllaDB node, the Server communicates with the Agent over HTTPS

The Server persists its data to a ScyllaDB cluster which can run locally, or can run on an external cluster.
Optionally, but recommended, you can add ScyllaDB Monitoring Stack to enable reporting of ScyllaDB Manager metrics and alerts.
ScyllaDB Manager comes with its own ScyllaDB Monitoring Dashboard.
The diagram below presents a view on ScyllaDB Manager managing multiple ScyllaDB Clusters.
ScyllaDB Manager Server has two connections with each ScyllaDB node:

* REST API connection - used to access ScyllaDB API and ScyllaDB Manager Agent
* (Optional) CQL connection - used for the ScyllaDB [Health Check](https://manager.docs.scylladb.com/branch-3.10/health-check.md#scylla-health-check)

![image](images/architecture.jpg)

## Installation

To proceed with installation go to [Install ScyllaDB Manager](https://manager.docs.scylladb.com/branch-3.10/install-scylla-manager.md).
