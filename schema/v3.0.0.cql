ALTER TYPE schedule ADD cron text;
ALTER TYPE schedule ADD retry_wait bigint;

-- CALL rewriteHealthCheck30;
