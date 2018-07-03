CREATE KEYSPACE IF NOT EXISTS test_ks_all_dc_1 WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': 3, 'dc2': 3};
CREATE KEYSPACE IF NOT EXISTS test_ks_all_dc_2 WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': 3, 'dc2': 3};
CREATE KEYSPACE IF NOT EXISTS test_ks_dc1 WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': 3};
CREATE KEYSPACE IF NOT EXISTS test_ks_dc2 WITH replication = {'class': 'NetworkTopologyStrategy', 'dc2': 3};

CREATE TABLE IF NOT EXISTS test_ks_all_dc_1.test_table (id int PRIMARY KEY);
CREATE TABLE IF NOT EXISTS test_ks_all_dc_2.test_table (id int PRIMARY KEY);
CREATE TABLE IF NOT EXISTS test_ks_dc1.test_table (id int PRIMARY KEY);
CREATE TABLE IF NOT EXISTS test_ks_dc2.test_table (id int PRIMARY KEY);
