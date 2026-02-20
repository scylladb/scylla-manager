FROM scylladb/scylla-manager-agent:latest as agent
FROM scylladb/scylla:latest

COPY --from=agent /usr/bin/scylla-manager-agent /usr/bin/

USER root

RUN cat > /etc/supervisord.conf.d/scylla-manager-agent.conf <<'EOF'
[program:scylla-manager-agent]
command=/usr/bin/scylla-manager-agent
autorestart=true
stdout_logfile=/dev/stdout
stdout_logfile_maxbytes=0
stderr_logfile=/dev/stderr
stderr_logfile_maxbytes=0
EOF

RUN mkdir -p /etc/scylla-manager-agent && cat > /etc/scylla-manager-agent/scylla-manager-agent.yaml <<'EOF'
auth_token: token
s3:
    access_key_id: minio
    secret_access_key: minio123
    provider: Minio
    endpoint: http://minio:9000
EOF

USER scylla
