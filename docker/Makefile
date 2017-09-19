COMPOSE=source ./.env && docker-compose
CQLSH=cqlsh --cqlversion=3.3.1 172.16.1.10

# up starts dc1 and dc2.
.PHONY: up
up:
	@$(COMPOSE) up -d dc1_seed_node
	@$(COMPOSE) up -d dc2_seed_node
	@$(COMPOSE) up -d --scale dc1_node=2 dc1_node
	@$(COMPOSE) up -d --scale dc2_node=2 dc2_node

# down stops running scylla cluster.
.PHONY: down
down:
	@$(COMPOSE) down

# init initialises database by running init.cql.
.PHONY: init
init:
	$(CQLSH) -f init.cql
	$(CQLSH) -f ../schema/cql/001_initial_schema.cql -k scylla_management

# bash opens a bash shell in seed node.
bash:
	@$(COMPOSE) exec dc1_seed_node bash

# cqlsh opens a bash shell in seed node.
cqlsh:
	@$(CQLSH)

# bash opens a bash shell in seed node.
status:
	@$(COMPOSE) exec dc1_seed_node nodetool status
