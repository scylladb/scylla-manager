CREATE KEYSPACE IF NOT EXISTS test_keyspace_rf3 WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': 3, 'dc2': 3};
CREATE KEYSPACE IF NOT EXISTS test_keyspace_rf2 WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': 2, 'dc2': 2};
CREATE KEYSPACE IF NOT EXISTS test_keyspace_dc1_rf3 WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': 3};
CREATE KEYSPACE IF NOT EXISTS test_keyspace_dc1_rf2 WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': 2};
CREATE KEYSPACE IF NOT EXISTS test_keyspace_dc2_rf3 WITH replication = {'class': 'NetworkTopologyStrategy', 'dc2': 3};
CREATE KEYSPACE IF NOT EXISTS test_keyspace_dc2_rf2 WITH replication = {'class': 'NetworkTopologyStrategy', 'dc2': 2};

CREATE TABLE IF NOT EXISTS test_keyspace_rf3.void1 (id int PRIMARY KEY);
CREATE TABLE IF NOT EXISTS test_keyspace_rf2.void1 (id int PRIMARY KEY);
CREATE TABLE IF NOT EXISTS test_keyspace_dc1_rf3.void1 (id int PRIMARY KEY);
CREATE TABLE IF NOT EXISTS test_keyspace_dc1_rf2.void1 (id int PRIMARY KEY);
CREATE TABLE IF NOT EXISTS test_keyspace_dc2_rf3.void1 (id int PRIMARY KEY);
CREATE TABLE IF NOT EXISTS test_keyspace_dc2_rf2.void1 (id int PRIMARY KEY);

ALTER KEYSPACE system_auth WITH REPLICATION = {'class': 'NetworkTopologyStrategy', 'dc1': 3, 'dc2': 3}