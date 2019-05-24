all: help

COMPOSE=docker-compose
CQLSH=cqlsh --cqlversion=3.3.1

include .env

.PHONY: build
build: ## Build custom docker images
	make -C scylla
	docker build --build-arg=SCYLLA_VERSION=$(SCYLLA_VERSION) -t scylladb/scylla-ssh:$(SCYLLA_VERSION) scylla

.PHONY: up
up: ## Start docker containers
	@echo "==> Starting containers"
	@mkdir -p $(MINIO_DATA_DIR)
	@$(COMPOSE) up -d
	@echo "==> Waiting for cluster"
	@until $(CQLSH) 192.168.100.11 -e "DESCRIBE SCHEMA" > /dev/null; do sleep 2; done
	@echo "==> Creating tables"
	@$(CQLSH) 192.168.100.11 -f init.cql
	@echo "==> Installing root SSH key"
	@./install_root_key
	@./update_agent

.PHONY: down
down: ## Stop docker containers
	@echo "==> Stopping containers"
	@$(COMPOSE) down --volumes --remove-orphans

.PHONY: status
status: ## Cluster nodetool status
	@$(COMPOSE) exec dc1_node_1 nodetool status

.PHONY: cqlsh
cqlsh: ## CQL shell to manager node
	@$(CQLSH) 192.168.100.100

.PHONY: help
help:
	@awk -F ':|##' '/^[^\t].+?:.*?##/ {printf "\033[36m%-25s\033[0m %s\n", $$1, $$NF}' $(MAKEFILE_LIST)
