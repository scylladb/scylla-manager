# Grafana dashboards

The Scylla Manager dashboard lives in the ScyllaDB Monitoring stack
([scylla-monitoring](https://github.com/scylladb/scylla-monitoring)), as
`grafana/scylla-manager.template.json`, and is developed there.
`scylla-manager.json` here is that dashboard built at a pinned
scylla-monitoring commit, so that the development environment shows exactly
what users run.

To pick up a change merged in scylla-monitoring, bump `SCYLLA_MONITORING_REF`
in `update-dashboard.sh`, then run

    make update-monitoring-dashboard

and commit both files. The build runs in a container, so only docker is
needed. To build another scylla-monitoring commit or branch, e.g. the latest,
pass it as `REF`: `make update-monitoring-dashboard REF=master`.

A dashboard change can be prototyped here, against the development
environment's metrics, but it lands through a scylla-monitoring PR, made in
the template there: `scylla-manager.json` is a build output, and a later
update overwrites any edit made to it here, so do not commit hand edits.

The development environment loads every `*.json` in this directory into its
own Grafana (http://localhost:3000, "Scylla Manager" folder) within 10
seconds, with no restart. Grafana is not started by `make start-dev-env` -
run `make start-dev-env-monitoring` to add it, and node-exporter with it. It
runs the Grafana version and feature toggles of the monitoring stack, and its
Prometheus datasource is named `prometheus`, as the stack's dashboards expect.

A dashboard can be edited in Grafana's UI to try something out, but saving is
refused: to keep a change, make it in scylla-monitoring.
