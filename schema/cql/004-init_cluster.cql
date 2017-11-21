-- Cluster

CREATE TABLE cluster (
    id uuid,
    name text,
    hosts set<text>,
    shard_count int,
    PRIMARY KEY (id)
);
