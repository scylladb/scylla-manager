all: help

COMPOSE=docker-compose
CQLSH=cqlsh --cqlversion=3.3.1

include .env

.PHONY: build
build: ## Build custom docker image
	@echo "==> Building custom Scylla image for testing"
	@make -C scylla
	docker build --network=host --build-arg=SCYLLA_VERSION=$(SCYLLA_VERSION) -t scylladb/scylla-ssh:$(SCYLLA_VERSION) scylla

.PHONY: up
up: ## Start docker containers
	@echo "==> Starting containers"
	@mkdir -p $(MINIO_DATA_DIR)
	@. ./.env && $(COMPOSE) up -d
	@echo "==> Waiting for cluster"
	@until $(CQLSH) 192.168.100.11 -e "DESCRIBE SCHEMA" > /dev/null; do sleep 2; done
	@echo "==> Creating tables"
	@$(CQLSH) 192.168.100.11 -f init.cql
	@echo "==> Installing root SSH key"
	@./install_root_key

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

.PHONY: cqlsh
cqlsh: ## CQL shell to manager node
	@$(CQLSH) 192.168.100.100

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
