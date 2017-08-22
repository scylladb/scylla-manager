DROP KEYSPACE IF EXISTS scylla_management;
CREATE KEYSPACE scylla_management WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': 3, 'dc2': 3};
