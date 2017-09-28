CREATE KEYSPACE IF NOT EXISTS {{.Keyspace}} WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
