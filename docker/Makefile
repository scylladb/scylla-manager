CQLSH=cqlsh --cqlversion=3.3.1 172.16.0.10

# up starts a 3 node scylla cluster.
.PHONY: up
up:
	docker-compose up -d --scale scylla-node=2

# down stops running scylla cluster.
.PHONY: down
down:
	docker-compose down

# init initialises database by running init.cql.
.PHONY: init
init:
	$(CQLSH) -f init.cql

# bash opens a bash shell in seed node.
bash:
	@docker-compose exec scylla-seed-node bash

# cqlsh opens a bash shell in seed node.
cqlsh:
	@$(CQLSH)

# bash opens a bash shell in seed node.
status:
	@docker-compose exec scylla-seed-node nodetool status

# get-tools installs all the required tools for other targets.
.PHONY: get-tools
	sudo pip install -u cqlsh
	sudo pip install -u docker-compose
