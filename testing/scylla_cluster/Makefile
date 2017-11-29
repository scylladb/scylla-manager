COMPOSE=source ./.env && docker-compose
CQLSH=cqlsh --cqlversion=3.3.1 172.16.1.100

.PHONY: up
up:
	@$(COMPOSE) up -d mgmt_node
	@$(COMPOSE) up -d dc1_seed_node
	@$(COMPOSE) up -d dc2_seed_node
	@$(COMPOSE) up -d --scale dc1_node=2 dc1_node
	@$(COMPOSE) up -d --scale dc2_node=2 dc2_node

.PHONY: down
down:
	@$(COMPOSE) down

.PHONY: status
status:
	@$(COMPOSE) exec dc1_seed_node nodetool status scylla_management

.PHONY: bash
bash:
	@$(COMPOSE) exec dc1_seed_node bash

.PHONY: cqlsh
cqlsh:
	@$(CQLSH)

