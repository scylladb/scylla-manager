COMPOSE=./docker-compose
CQLSH=cqlsh --cqlversion=3.3.1

include .env

.PHONY: build
build:
	docker build --build-arg=SCYLLA_VERSION=$(SCYLLA_VERSION) -t scylladb/scylla-ssh:$(SCYLLA_VERSION) scylla

.PHONY: up
up:
	@$(COMPOSE) up -d

.PHONY: init
init:
	@echo "==> Installing keys"
	@cd scylla-manager && ./install_keys.sh
	@echo "==> Initialising DB"
	@$(CQLSH) 192.168.51.11 -f init.cql

.PHONY: down
down:
	@$(COMPOSE) down --volumes --remove-orphans

.PHONY: status
status:
	@$(COMPOSE) exec dc1_node_1 nodetool status

.PHONY: cqlsh
cqlsh:
	@$(CQLSH) 192.168.100.100
