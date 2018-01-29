COMPOSE=docker-compose

.PHONY: up
up:
	@$(COMPOSE) up -d

.PHONY: down
down:
	@$(COMPOSE) down --volumes --remove-orphans
