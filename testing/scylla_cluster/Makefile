COMPOSE=docker-compose
CQLSH=cqlsh --cqlversion=3.3.1

include .env

.PHONY: build
build:
	docker build --build-arg=SCYLLA_VERSION=$(SCYLLA_VERSION) -t scylladb/scylla-ssh:$(SCYLLA_VERSION) .

.PHONY: up
up:
	@$(COMPOSE) up -d manager_node
	@$(COMPOSE) up -d dc1_seed_node
	@$(COMPOSE) up -d dc2_seed_node
	@$(COMPOSE) up -d --scale dc1_node=2 dc1_node
	@$(COMPOSE) up -d --scale dc2_node=2 dc2_node

.PHONY: down
down:
	@$(COMPOSE) down --volumes --remove-orphans

.PHONY: status
status:
	@echo "==> Backend"
	@$(COMPOSE) exec manager_node nodetool status | grep -e "^[UD]" | grep -v Datacenter
	@echo "==> Managed Cluster"
	@$(COMPOSE) exec dc1_seed_node nodetool status | grep -e "^[UD]" | grep -v Datacenter

.PHONY: cqlsh
cqlsh:
	@$(CQLSH) 172.16.1.100

.PHONY: test-keyspace
test-keyspace:
	@$(CQLSH) 172.16.1.10 -e "CREATE KEYSPACE test_repair WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': 3, 'dc2': 3}"
	@$(CQLSH) 172.16.1.10 -e "CREATE TABLE test_repair.test_table (id int PRIMARY KEY)"
