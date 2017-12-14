-- Cluster

CREATE TABLE IF NOT EXISTS cluster (
    id uuid,
    name text,
    hosts set<text>,
    shard_count int,
    PRIMARY KEY (id)
);
