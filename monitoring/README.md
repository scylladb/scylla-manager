# Grafana dashboards

Dashboards built on the metrics Scylla Manager exports.

The dashboards users actually run live in the ScyllaDB Monitoring stack
(scylla-monitoring), as `grafana/scylla-manager.template.json`, built there
per Manager major version. The ones here are what this repository proposes
to it, kept in a form that can be developed and reviewed against a running
Manager.

The development environment loads every `*.json` in this directory into its
own Grafana (http://localhost:3000, "Scylla Manager" folder) within 10
seconds, with no restart. Grafana is not started by `make start-dev-env` -
run `make start-dev-env-monitoring` to add it, and node-exporter with it.

A dashboard can also be edited in Grafana's UI to try something out, but the
files here are the source of truth: the provider does not write UI changes
back, and Grafana's own database is wiped by `make down`. To keep a change,
export it (Dashboard settings -> JSON Model, or Share -> Export) and save it
here.

The Prometheus datasource is referenced by the fixed uid
"scylla-manager-prometheus", which the development environment provisions.
Importing one of these files into any other Grafana needs a datasource with
that uid, or the panels repointed after the import.
