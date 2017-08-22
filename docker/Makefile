COMPOSE=source ./.env && docker-compose
CQLSH=cqlsh --cqlversion=3.3.1 172.16.1.10

# up-dc2 starts dc1 and dc2.
.PHONY: up
up: up-dc1 up-dc2

# up-dc2 starts dc1.
.PHONY: up-dc1
up-dc1:
	@$(COMPOSE) up -d dc1_seed_node
	@$(COMPOSE) up -d --scale dc1_node=2 dc1_node

# up-dc2 starts dc2.
.PHONY: up-dc2
up-dc2:
	@$(COMPOSE) up -d dc2_seed_node
	@$(COMPOSE) up -d --scale dc2_node=2 dc2_node

# down stops running scylla cluster.
.PHONY: down
down:
	@$(COMPOSE) down

# init initialises database by running init.cql.
.PHONY: init
init:
	$(CQLSH) -f init.cql
	$(CQLSH) -f ../schema/001_initial_schema.cql

# bash opens a bash shell in seed node.
bash:
	@$(COMPOSE) exec dc1_seed_node bash

# cqlsh opens a bash shell in seed node.
cqlsh:
	@$(CQLSH)

# bash opens a bash shell in seed node.
status:
	@$(COMPOSE) exec dc1_seed_node nodetool status

# get-tools installs all the required tools for other targets.
.PHONY: get-tools
	sudo pip install -u cqlsh
	sudo pip install -u COMPOSE
