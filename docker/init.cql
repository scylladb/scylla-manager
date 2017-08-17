CREATE KEYSPACE IF NOT EXISTS scylla_management WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3};
CREATE KEYSPACE IF NOT EXISTS test WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3};
