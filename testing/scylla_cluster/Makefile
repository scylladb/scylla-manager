COMPOSE=source ./.env && docker-compose
CQLSH=cqlsh --cqlversion=3.3.1 172.16.1.100

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
	@$(CQLSH)

