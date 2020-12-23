all: help

COMPOSE=docker-compose
DOCKER_NODE=scylla_manager_dc1_node_1_1
CQLSH=docker exec -t $(DOCKER_NODE) cqlsh -f /root/.cassandra/cqlshrc
CQLSH_PORT=9042
ifdef SSL_ENABLED
	CQLSH_SSL=--ssl
	CQLSH_PORT=9142
	SCYLLA_SSL_CONFIG=scylla/etc/server/scylla-ssl.yaml
endif
CQLSH_NODE=$(CQLSH) -u cassandra -p cassandra $(CQLSH_SSL) 192.168.100.11 $(CQLSH_PORT)

include .env

.PHONY: build
build: ## Build custom docker image
	@echo "==> Building custom Scylla image for testing"
	@docker build --network=host --build-arg=SCYLLA_VERSION=$(SCYLLA_VERSION) -t scylladb/scylla-ssh:$(SCYLLA_VERSION) scylla

.PHONY: up
up: ## Start docker containers
up:
	@echo "==> Generating encryption files"
	@cd scylla/certs && ./generate.sh
	@echo "==> Generating Scylla configuration"
ifdef SSL_ENABLED
	@yq merge -x scylla/etc/server/scylla.yaml $(SCYLLA_SSL_CONFIG) > scylla/scylla.yaml
else
	@cp scylla/etc/server/scylla.yaml scylla/scylla.yaml
endif
	@echo "==> Starting containers"
	@mkdir -p $(MINIO_DATA_DIR)
	@. ./.env && $(COMPOSE) up -d
	@echo "==> Waiting for cluster"
	@until $(CQLSH_NODE) -e "DESCRIBE SCHEMA" > /dev/null; do sleep 2; done
	@echo "==> Creating tables"
	@docker cp init.cql $(DOCKER_NODE):/tmp/
	@$(CQLSH_NODE) -f /tmp/init.cql
	@echo "==> Installing root SSH key"
	@./install_root_key
	@echo "==> Adding Minio user"
	@./minio/add_user.sh

.PHONY: down
down: ## Stop docker containers
	@echo "==> Stopping containers"
	@$(COMPOSE) down --volumes --remove-orphans

.PHONY: status
status: ## Cluster containers status and nodetool status
	@$(COMPOSE) ps
	@$(COMPOSE) exec dc1_node_1 nodetool status

.PHONY: logs
logs: ## Show logs
	@$(COMPOSE) logs --tail 10 -f $(SRV)

.PHONY: cqlsh-manager
cqlsh-manager: ## CQL shell to manager backend storage
	@$(CQLSH) 192.168.100.100

.PHONY: cqlsh-node
cqlsh-node: ## CQL shell to a managed node 192.168.100.11
	$(CQLSH_NODE)

.PHONY: drop-keyspace
drop-keyspace: ## Drop Scylla Manager keyspace
	@$(CQLSH) 192.168.100.100 -e "DROP KEYSPACE scylla_manager"

.PHONY: deploy-agent
deploy-agent: ## Update agent to the latest build on all nodes
	@./nodes_cp ../scylla-manager-agent.dev /usr/bin/scylla-manager-agent
	@./nodes_exec --silent mkdir -p /etc/scylla-manager-agent
	@./nodes_cp ./scylla-manager-agent/scylla-manager-agent.yaml /etc/scylla-manager-agent/scylla-manager-agent.yaml

.PHONY: restart-agent
restart-agent: ## Restart agent on all nodes
	@./nodes_exec supervisorctl restart scylla-manager-agent

.PHONY: help
help:
	@awk -F ':|##' '/^[^\t].+?:.*?##/ {printf "\033[36m%-25s\033[0m %s\n", $$1, $$NF}' $(MAKEFILE_LIST)
