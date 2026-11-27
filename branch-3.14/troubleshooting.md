# Troubleshooting

> * [Use custom port for ScyllaDB Manager Agent](#use-custom-port-for-scylladb-manager-agent)
> * [Add a Node to a Managed Cluster](#add-a-node-to-a-managed-cluster)
> * [Remove a Node from a Managed Cluster](#remove-a-node-from-a-managed-cluster)

## Use custom port for ScyllaDB Manager Agent

By default ScyllaDB Manager Agent starts API server on port 10001.
The following procedure changes the default port.

Note that this procedure needs to be repeated for each ScyllaDB node.

**Procedure**

1. Edit the `/etc/scylla-manager-agent/scylla-manager-agent.yaml`.
2. Uncomment and set `https_port` to match your desired port - **all nodes must use the same port**.
3. Save the file.
4. Restart ScyllaDB Manager Agent service.
   ```none
   sudo systemctl restart scylla-manager-agent
   ```
5. Verify the ScyllaDB Manager Agent is running.
   ```none
   sudo systemctl status scylla-manager-agent -l
   ● scylla-manager-agent.service - Scylla Manager Agent
     Loaded: loaded (/usr/lib/systemd/system/scylla-manager-agent.service; disabled; vendor preset: disabled)
     Active: active (running) since Wed 2019-10-30 10:46:51 UTC; 7s ago
       Main PID: 14670 (scylla-manager-)
       CGroup: /system.slice/scylla-manager-agent.service
              └─14670 /usr/bin/scylla-manager-agent
   ```

After that Change cluster port in ScyllaDB Manager.

```none
sctool cluster update -c <your cluster> --port <new port>
```

You can also pass the `--port` flag to [sctool cluster add](https://manager.docs.scylladb.com/stable/sctool/cluster.md#cluster-add) command when registering a new cluster.

## Add a Node to a Managed Cluster

Although ScyllaDB Manager is aware of all topology changes made within every cluster it manages, it cannot properly manage a cluster without establishing connections with every node in the cluster using the ScyllaDB Manager Agent which is on each managed node.

**Procedure**

1. [Add ScyllaDB Manager Agent](https://manager.docs.scylladb.com/stable/install-scylla-manager-agent.md#install-agent) to the new node. Use the **same** authentication token as you did for the other nodes in this cluster. Do not generate a new token.
2. Confirm the node / datacenter was added by checking its [sctool status](https://manager.docs.scylladb.com/stable/sctool/status.md#id1). From the node running ScyllaDB Manager server run the `sctool status` command, using the name of the managed cluster.
   ```none
   sctool status -c prod-cluster
   Datacenter: eu-west
   ╭────┬────────────┬───────────┬───────────┬───────────────┬──────────┬──────┬──────────┬────────┬──────────┬──────────────────────────────────────╮
   │    │ Alternator │ CQL       │ REST      │ Address       │ Uptime   │ CPUs │ Memory   │ Scylla │ Agent    │ Host ID                              │
   ├────┼────────────┼───────────┼───────────┼───────────────┼──────────┼──────┼──────────┼────────┼──────────┼──────────────────────────────────────┤
   │ UN │ UP (4ms)   │ UP (3ms)  │ UP (2ms)  │ 34.203.122.52 │ 237h2m1s │ 4    │ 15.43GiB │ 4.1.0  │ 2.2.0    │ 8bfd18f1-ac3b-4694-bcba-30bc272554df │
   │ UN │ UP (15ms)  │ UP (11ms) │ UP (12ms) │ 10.0.138.46   │ 237h2m1s │ 4    │ 15.43GiB │ 4.1.0  │ 2.2.0    │ 238acd01-813c-4c55-bd65-5219bb19bc20 │
   │ UN │ UP (17ms)  │ UP (5ms)  │ UP (7ms)  │ 10.0.196.204  │ 237h2m1s │ 4    │ 15.43GiB │ 4.1.0  │ 2.2.0    │ bde4581a-b25e-49fc-8cd9-1651d7683f80 │
   │ UN │ UP (10ms)  │ UP (4ms)  │ UP (5ms)  │ 10.0.66.115   │ 237h2m1s │ 4    │ 15.43GiB │ 4.1.0  │ 2.2.0    │ 918a52aa-cc42-43a4-a499-f7b1ccb53b18 │
   ╰────┴────────────┴───────────┴───────────┴───────────────┴──────────┴──────┴──────────┴────────┴──────────┴──────────────────────────────────────╯
   ```

## Remove a Node from a Managed Cluster

There is no need to perform any action in ScyllaDB Manager after removing a node or datacenter from a ScyllaDB cluster.
