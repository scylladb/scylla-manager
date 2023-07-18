all: help

COMPOSE      := docker-compose
CQLSH        := $(COMPOSE) exec scylla-manager-db cqlsh
CQLSH_NODE   := $(COMPOSE) exec -T dc1_node_1 cqlsh
NODETOOL     := $(COMPOSE) exec -T dc1_node_1 nodetool
SM_NODETOOL := $(COMPOSE) exec -T scylla-manager-db nodetool
YQ           := ../bin/yq
CURRENT_UID  := $(shell id -u)
CURRENT_GID  := $(shell id -g)

SCYLLA_VERSION?=5.1.13
IP_FAMILY?=IPV4

MINIO_ENDPOINT := http://192.168.200.99:9000
COMPOSE_FILE := docker-compose-ipv4.yaml
AGENT_CONFIG := scylla-manager-agent/scylla-manager-agent.yaml
PUBLIC_NET := 192.168.100.
SECOND_NET := 192.168.200.

ifeq ($(IP_FAMILY), IPV6)
	MINIO_ENDPOINT := http://[2001:0DB9:200::99]:9000
	COMPOSE_FILE := docker-compose-ipv6.yaml
	AGENT_CONFIG := scylla-manager-agent/scylla-manager-agent-ipv6.yaml
	PUBLIC_NET := 2001:0DB9:100::
	SECOND_NET := 2001:0DB9:200::
endif

SCYLLA_ARGS := --smp 2 --memory 2G --seeds $(SECOND_NET)11,$(SECOND_NET)21
SCYLLA_SECOND_CLUSTER_ARGS := --smp 2 --memory 2G --seeds $(SECOND_NET)31

export SCYLLA_ARGS
export SCYLLA_SECOND_CLUSTER_ARGS
export MINIO_ENDPOINT

include .env

.PHONY: build
build: ## Build custom docker image
	@echo "==> Building custom Scylla $(SCYLLA_VERSION) image for testing"
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
	@echo "==> Starting testing env with SCYLLA_VERSION=$(SCYLLA_VERSION) and IP_FAMILY=$(IP_FAMILY)"
# Scylla bootstap proceedure have requirements that leads us to follow certain recipe:
# 1. Spin up first node on the cluster
# 2. Spin up and join other seed node, which is first node from DC2
# 3. Spin up rest of the nodes
	@echo "==> Generating encryption files"
	@cd scylla/certs && ./generate.sh
	@echo "==> Generating Scylla configuration"
ifeq ($(SM_SSL_ENABLED),yes)
	@$(YQ) merge -x scylla/config/scylla.yaml scylla/config/scylla-ssl.yaml > scylla/scylla.yaml
	@$(YQ) merge -x scylla/config/scylla-second-cluster.yaml scylla/config/scylla-ssl.yaml > scylla/scylla-scylla-second-cluster.yaml
	@cp scylla/config/cqlshrc-ssl scylla/cqlshrc
else
	@cp scylla/config/scylla.yaml scylla/scylla.yaml
	@cp scylla/config/scylla-second-cluster.yaml scylla/scylla-second-cluster.yaml
	@cp scylla/config/cqlshrc scylla/cqlshrc
endif
	@echo "==> Starting containers"
	mkdir -p $(MINIO_DATA_DIR)
	@. ./.env && CURRENT_UID=$(CURRENT_UID) CURRENT_GID=$(CURRENT_GID) $(COMPOSE) -f docker-compose.yaml -f $(COMPOSE_FILE) up -d dc1_node_1 dc2_node_1 second_cluster_dc1_node_1
	$(COMPOSE) exec -T --privileged dc1_node_1 sudo bash -c 'echo "fs.aio-max-nr = 1048579" > /etc/sysctl.d/50-scylla.conf'
	$(COMPOSE) exec -T --privileged dc1_node_1 sudo sysctl -p /etc/sysctl.d/50-scylla.conf
	@echo "==> Waiting for first nodes from DC1 and DC2 to spin up"
	@until [ 2 -le $$($(NODETOOL) status | grep -c "UN") ]; do echo -n "."; sleep 2; done ; echo ""
	@echo "==> First nodes from DC1 and DC2 are ready"
	@. ./.env && CURRENT_UID=$(CURRENT_UID) CURRENT_GID=$(CURRENT_GID) $(COMPOSE) -f docker-compose.yaml -f $(COMPOSE_FILE) up -d dc1_node_2 dc2_node_2 second_cluster_dc1_node_2
	@echo "==> Waiting for second nodes from DC1 and DC2 to spin up"
	@until [ 4 -le $$($(NODETOOL) status | grep -c "UN") ]; do echo -n "."; sleep 2; done ; echo ""
	@echo "==> Second nodes from DC1 and DC2 are ready"
	@. ./.env && CURRENT_UID=$(CURRENT_UID) CURRENT_GID=$(CURRENT_GID) $(COMPOSE) -f docker-compose.yaml -f $(COMPOSE_FILE) up -d
	@echo "==> Waiting for sm db cluster"
	@until [ 1 -le $$($(SM_NODETOOL) status | grep -c "UN") ]; do echo -n "."; sleep 2; done ; echo ""
	@echo "==> SM cluster is ready"

	@echo "==> Waiting for rest of the cluster"
	@until [ 6 -le $$($(NODETOOL) status | grep -c "UN") ]; do echo -n "."; sleep 2; done ; echo ""
	@echo "==> Cluster is ready"

	@./nodes_exec "rm /root/.cqlshrc || true"
	@echo "==> Adding Minio user"
	./minio/add_user.sh
	@echo "==> Initialising cluster"
	@$(CQLSH_NODE) $(PUBLIC_NET)11 -e "ALTER KEYSPACE system_auth WITH REPLICATION = {'class': 'NetworkTopologyStrategy', 'dc1': 3, 'dc2': 3}"

.PHONY: down
down: ## Stop docker containers
	@echo "==> Stopping containers"
	@$(COMPOSE) down --volumes --remove-orphans
	@docker network prune -f

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
cqlsh-node: IP=$(PUBLIC_NET)11
cqlsh-node:
	$(CQLSH_NODE) $(IP)

.PHONY: create-tables
create-tables: ## Execute init.cql
	@$(CQLSH_NODE) $(PUBLIC_NET)11 -e "`cat init.cql`"

.PHONY: drop-keyspace
drop-keyspace: ## Drop Scylla Manager keyspace
	@$(CQLSH) -e "DROP KEYSPACE scylla_manager"

.PHONY: deploy-agent
deploy-agent: ## Update agent to the latest build on all nodes
	@./nodes_cp ../scylla-manager-agent.dev /usr/bin/scylla-manager-agent
	@./nodes_cp $(AGENT_CONFIG) /etc/scylla-manager-agent/scylla-manager-agent.yaml

.PHONY: restart-agent
restart-agent: ## Restart agent on all nodes
	@./nodes_exec supervisorctl restart scylla-manager-agent

.PHONY: help
help:
	@awk -F ':|##' '/^[^\t].+?:.*?##/ {printf "\033[36m%-25s\033[0m %s\n", $$1, $$NF}' $(MAKEFILE_LIST)
