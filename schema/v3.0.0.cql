ALTER TYPE schedule ADD cron text;
ALTER TYPE schedule ADD retry_initial_interval bigint;

-- CALL rewriteHealthCheck30;
