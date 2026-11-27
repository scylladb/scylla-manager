# Compatibility Matrix

The following table shows which version of ScyllaDB Manager supports which versions of ScyllaDB.

|   ScyllaDB Manager Version | ScyllaDB Version                           |
|----------------------------|--------------------------------------------|
|                       3.14 | 2025.1, 2026.1, 2026.2, 2026.3, 2026.4     |
|                       3.13 | 2025.1, 2026.1, 2026.2, 2026.3             |
|                       3.12 | 2025.1, 2025.4, 2026.1, 2026.2             |
|                       3.11 | 2025.1, 2025.4, 2026.1, 2026.2             |
|                       3.1  | 2025.1, 2025.4, 2026.1                     |
|                       3.9  | 2024.1, 2025.1, 2025.3, 2025.4, 2026.1     |
|                       3.8  | 2024.1, 2025.1, 2025.2, 2025.3, 2025.4     |
|                       3.7  | 2024.1, 2025.1, 2025.2, 2025.3, 2025.4     |
|                       3.6  | 2024.1, 2025.1, 2025.2, 2025.3             |
|                       3.5  | 2024.1, 2024.2, 2025.1, 2025.2             |
|                       3.4  | 5.4, 6.0, 6.1, 6.2, 2023.1, 2024.1, 2024.2 |

## Unsupported ScyllaDB features

The following ScyllaDB features are not supported on clusters managed by ScyllaDB Manager.

<a id="automatic-repair-unsupported"></a>

### Automatic repair

ScyllaDB [automatic repair](https://docs.scylladb.com/manual/stable/features/automatic-repair.html) must stay disabled on clusters managed by ScyllaDB Manager.
This applies to the `auto_repair_enabled` option set via CQL on the cluster, keyspace or table level, and to the `auto_repair_enabled_default` option set in `scylla.yaml`.

Automatic repair schedules tablet repairs on its own and ScyllaDB Manager does not coordinate with it.
It can cause conflicts with the [repair](https://manager.docs.scylladb.com/stable/repair/index.md) and [restore](https://manager.docs.scylladb.com/stable/restore/index.md) tasks.
