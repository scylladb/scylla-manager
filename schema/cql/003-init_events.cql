-- Events

CREATE TABLE event (
    external_id text,
    type text,
    event_time timestamp,
    message text,
    PRIMARY KEY((external_id, type), event_time)
) WITH CLUSTERING ORDER BY (event_time DESC);
