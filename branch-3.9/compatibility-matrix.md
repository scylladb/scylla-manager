# Compatibility Matrix

The following table shows which version of ScyllaDB Manager supports which versions of ScyllaDB.

#### WARNING
Restoring schema into a cluster with ScyllaDB **5.4.X** or **2024.1.X** with `consistent_cluster_management: true` isn’t supported. Please see the following [workaround](https://manager.docs.scylladb.com/branch-3.9/restore/old-restore-schema.md#restore-schema-workaround).

|   ScyllaDB Manager Version | ScyllaDB Version                           |
|----------------------------|--------------------------------------------|
|                        3.9 | 2024.1, 2025.1, 2025.3, 2025.4, 2026.1     |
|                        3.8 | 2024.1, 2025.1, 2025.2, 2025.3, 2025.4     |
|                        3.7 | 2024.1, 2025.1, 2025.2, 2025.3, 2025.4     |
|                        3.6 | 2024.1, 2025.1, 2025.2, 2025.3             |
|                        3.5 | 2024.1, 2024.2, 2025.1, 2025.2             |
|                        3.4 | 5.4, 6.0, 6.1, 6.2, 2023.1, 2024.1, 2024.2 |
