all: help

COMPOSE    := docker-compose
CQLSH      := $(COMPOSE) exec scylla-manager-db cqlsh
CQLSH_NODE := $(COMPOSE) exec dc1_node_1 cqlsh
YQ         := ../bin/yq

include .env

.PHONY: build
build: ## Build custom docker image
	@echo "==> Building custom Scylla image for testing"
	@docker build --network=host \
		--build-arg=SCYLLA_IMAGE=$(SCYLLA_IMAGE) \
		--build-arg=SCYLLA_VERSION=$(SCYLLA_VERSION) \
		-t scylladb/scylla-agent:$(SCYLLA_VERSION) \
		scylla
	@echo "==> Building development image for Scylla Manager Server"
	@docker build --network=host -t scylladb/scylla-manager-dev scylla-manager

.PHONY: up
up: ## Start docker containers
up:
	@echo "==> Generating encryption files"
	@cd scylla/certs && ./generate.sh
	@echo "==> Generating Scylla configuration"
ifeq ($(SM_SSL_ENABLED),yes)
	@$(YQ) merge -x scylla/config/scylla.yaml scylla/config/scylla-ssl.yaml > scylla/scylla.yaml
	@cp scylla/config/cqlshrc-ssl scylla/cqlshrc
else
	@cp scylla/config/scylla.yaml scylla/scylla.yaml
	@cp scylla/config/cqlshrc scylla/cqlshrc
endif
	@echo "==> Starting containers"
	@mkdir -p $(MINIO_DATA_DIR)
	@. ./.env && $(COMPOSE) up -d
	@echo "==> Waiting for cluster"
	@until $(CQLSH_NODE) 192.168.100.11 -e "DESCRIBE SCHEMA" > /dev/null; do echo ""; sleep 2; done
	@./nodes_exec rm /root/.cqlshrc
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

.PHONY: agent-logs
agent-logs: ## Show logs for the managed scylla cluster
	@$(MAKE) logs SRV='dc1_node_1 dc1_node_2 dc1_node_3 dc2_node_1 dc2_node_2 dc2_node_3'

.PHONY: logs
logs: ## Show logs for a service specified with SRV parameter or all services if not set
	@$(COMPOSE) logs --tail 10 -f $(SRV)

.PHONY: cqlsh-manager
cqlsh-manager: ## CQL shell to manager backend storage
	@$(CQLSH)

.PHONY: cqlsh-node
cqlsh-node: ## CQL shell to a managed node 192.168.100.11
cqlsh-node: IP=192.168.100.11
cqlsh-node:
	$(CQLSH_NODE) $(IP)

.PHONY: create-tables
create-tables: ## Execute init.cql
	@$(CQLSH_NODE) 192.168.100.11 -e "`cat init.cql`"

.PHONY: drop-keyspace
drop-keyspace: ## Drop Scylla Manager keyspace
	@$(CQLSH) -e "DROP KEYSPACE scylla_manager"

.PHONY: deploy-agent
deploy-agent: ## Update agent to the latest build on all nodes
	@./nodes_cp ../scylla-manager-agent.dev /usr/bin/scylla-manager-agent
	@./nodes_cp ./scylla-manager-agent/scylla-manager-agent.yaml /etc/scylla-manager-agent/scylla-manager-agent.yaml

.PHONY: restart-agent
restart-agent: ## Restart agent on all nodes
	@./nodes_exec supervisorctl restart scylla-manager-agent

.PHONY: help
help:
	@awk -F ':|##' '/^[^\t].+?:.*?##/ {printf "\033[36m%-25s\033[0m %s\n", $$1, $$NF}' $(MAKEFILE_LIST)
