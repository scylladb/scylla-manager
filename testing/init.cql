CREATE KEYSPACE IF NOT EXISTS test_repair WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': 3, 'dc2': 3};

CREATE TABLE IF NOT EXISTS test_repair.test_table (id int PRIMARY KEY);
