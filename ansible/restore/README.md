# Restore Scylla Manager backup

This is an ansible playbook that lets you clone a cluster from a Scylla Manager backup.
It requires Scylla Manager 2.4 or newer.

## Prerequisites

1. New cluster with the same number of nodes as the source cluster.
2. Scylla Manager Agent installed on all the nodes.
3. `jq` command installed on all the nodes.
4. Access to the backup location from all the nodes.

## Parameters

All restore parameters shall be put to `vars.yaml` file.
Copy `vars.yaml.example` as `vars.yaml` and change parameters to match your clusters. 

### IP to host ID mapping

SSH to one of the nodes and execute the following command:

```bash
scylla-manager-agent download-files -L <backup-location> --list-nodes
```

it gives you information about all the clusters and nodes available in the backup location.

Example output:

```
Cluster: prod (a9dcc6e1-17dc-4520-9e03-0a92011c823c)
AWS_EU_CENTRAL_1:
- 18.198.164.180 (7e68421b-acb1-44a7-a1a8-af7eaf1bb482)
- 3.122.35.120 (adc0a3ce-dade-4672-981e-26f91a3d35cb)
- 3.127.175.215 (2a575244-3e3c-44a1-a526-da4394f9525e)
- 3.65.85.108 (82f0f486-370d-4cfd-90ac-46464c8012cb)
- 3.66.209.145 (9ee92c19-5f78-4865-a287-980218963d96)
- 3.66.44.239 (aff05f79-7c69-4ecf-a827-5ea790a0fdc6)

Cluster: test (da5721cd-e2eb-4d10-a3a7-f729b8f72abf)
AWS_EU_CENTRAL_1:
- 3.123.55.215 (4001206a-3377-40cb-abd4-d38aad5dec41)
- 3.127.1.189 (c6466011-02f9-49dd-8951-c32028dfc6f1)
- 3.64.219.214 (bc39bb07-7a21-41cd-b576-51d44c1a694a)
```

For each node IP in the new cluster you need to assign a host ID (UUID in the listing above).
The mapping must be put into `host_id` variable in `vars.yaml` file.

### Snapshot tag

To list available snapshot tags for a node use `--list-snapshots` flag.

```bash
scylla-manager-agent download-files -L <backup-location> --list-snapshots -n <host-id>
```

You can filter snapshot tags containing a specific keyspaces or tables by using glob patterns.
The parameter `-K, --keyspace <list of glob patterns to find keyspaces>` lets you do that.
It works the same way as in scheduling backups or repairs with Scylla Manager.

The snapshot ID must be put into `snapshot_tag` variable in `vars.yaml` file.

## Inventory

Put public IP addresses of all nodes to `hosts` file.

## Running

Rut the playbook:

```bash
ansible-playbook -i hosts -e @vars.yaml restore.yaml
```
