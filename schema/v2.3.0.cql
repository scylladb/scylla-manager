-- Drawer

CREATE TABLE drawer (
    cluster_id uuid,
    key text,
    value blob,
    PRIMARY KEY (cluster_id, key)
);
