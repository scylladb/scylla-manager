DROP KEYSPACE IF EXISTS scylla_management;
CREATE KEYSPACE scylla_management WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3};
