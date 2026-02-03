all: help

COMPOSE      := docker compose
CQLSH        := $(COMPOSE) exec scylla-manager-db cqlsh
CQLSH_NODE   := $(COMPOSE) exec -T dc1_node_1 cqlsh
NODETOOL     := $(COMPOSE) exec -T dc1_node_1 nodetool
SECOND_NODETOOL := $(COMPOSE) exec -T second_cluster_dc1_node_1 nodetool
SM_NODETOOL  := $(COMPOSE) exec -T scylla-manager-db nodetool
YQ           := ../bin/yq
CURRENT_UID  := $(shell id -u)
CURRENT_GID  := $(shell id -g)

SCYLLA_VERSION?=scylla:latest
IP_FAMILY?=IPV4
RAFT_SCHEMA?=none
TABLETS?=enabled

MINIO_HOST := 192.168.200.99
MINIO_ENDPOINT := https://192.168.200.99:9000
COMPOSE_FILE := docker-compose-ipv4.yaml
AGENT_CONFIG := scylla-manager-agent/scylla-manager-agent.yaml
PUBLIC_NET := 192.168.100.
SECOND_NET := 192.168.200.
MINIO_CERT_DIR := ./minio/certs_ipv4

ifeq ($(IP_FAMILY), IPV6)
	# As currently scylla can't handle ipv6 object storage endpoints,
	# we don't configure them for ipv6 test env.
	MINIO_HOST := xxx
	MINIO_ENDPOINT := https://[2001:0DB9:200::99]:9000
	COMPOSE_FILE := docker-compose-ipv6.yaml
	AGENT_CONFIG := scylla-manager-agent/scylla-manager-agent-ipv6.yaml
	PUBLIC_NET := 2001:0DB9:100::
	SECOND_NET := 2001:0DB9:200::
	MINIO_CERT_DIR := ./minio/certs_ipv6
endif

SCYLLA_ARGS := --smp 2 --memory 2G --seeds $(SECOND_NET)11
SCYLLA_SECOND_CLUSTER_ARGS := --smp 2 --memory 2G --seeds $(SECOND_NET)31

export SCYLLA_ARGS
export SCYLLA_SECOND_CLUSTER_ARGS
export MINIO_ENDPOINT
export MINIO_CERT_DIR

include .env

.PHONY: build
build: ## Build custom docker image
	@echo "==> Building custom Scylla $(SCYLLA_VERSION) image for testing"
	@docker build --network=host \
		--build-arg=SCYLLA_VERSION=$(SCYLLA_VERSION) \
		-t scylladb/agent-$(SCYLLA_VERSION) \
		scylla
	@echo "==> Building development image for Scylla Manager Server"
	@docker build --network=host -t scylladb/scylla-manager-dev scylla-manager

.PHONY: up
up: ## Start docker containers
up:
	@echo "==> Starting testing env with SCYLLA_VERSION=$(SCYLLA_VERSION) and IP_FAMILY=$(IP_FAMILY) and RAFT_SCHEMA=$(RAFT_SCHEMA) and TABLETS=$(TABLETS) and SSL_ENABLED=$(SSL_ENABLED)"
# Scylla bootstrap procedure have requirements that leads us to follow certain recipe:
# 1. Spin up first node on the cluster
# 2. Spin up and join other seed node, which is first node from DC2
# 3. Spin up rest of the nodes
	@echo "==> Generating encryption files"
	@cd scylla/certs && ./generate.sh
	@echo "==> Generating Scylla configuration"
	@cp scylla/config/scylla.yaml scylla/scylla.yaml
	@$(YQ) write -i scylla/scylla.yaml 'object_storage_endpoints[0].(name)' $(MINIO_HOST)

ifeq ($(SSL_ENABLED),true)
	# disable non-ssl ports
	@$(YQ) delete -i scylla/scylla.yaml 'native_transport_port'
	@$(YQ) delete -i scylla/scylla.yaml 'alternator_port'
	# merge into scylla.yaml values from config/scylla-ssl.yaml with overwrite option (-x)
	@$(YQ) merge -i -x scylla/scylla.yaml scylla/config/scylla-ssl.yaml
	@cp scylla/config/cqlshrc-ssl scylla/cqlshrc
else 
	@cp scylla/config/cqlshrc scylla/cqlshrc
endif

ifeq ($(RAFT_SCHEMA),enabled)
	@$(YQ) write -i scylla/scylla.yaml 'consistent_cluster_management' true
endif
ifeq ($(RAFT_SCHEMA),disabled)
	@$(YQ) write -i scylla/scylla.yaml 'consistent_cluster_management' false
endif
ifeq ($(TABLETS),enabled)
	@$(YQ) write -i scylla/scylla.yaml 'enable_tablets' true
	@$(YQ) write -i scylla/scylla.yaml 'tablets_mode_for_new_keyspaces' enabled
endif
ifeq ($(TABLETS),disabled)
	@$(YQ) write -i scylla/scylla.yaml 'enable_tablets' false
	@$(YQ) write -i scylla/scylla.yaml 'tablets_mode_for_new_keyspaces' disabled
endif
	@cp scylla/scylla.yaml scylla/scylla-second-cluster.yaml
	@$(YQ) write -i scylla/scylla-second-cluster.yaml 'cluster_name' 'Managed Other Cluster'

	@echo "==> Starting containers"
	mkdir -p $(MINIO_DATA_DIR)

	@. ./.env && CURRENT_UID=$(CURRENT_UID) CURRENT_GID=$(CURRENT_GID) $(COMPOSE) -f docker-compose.yaml -f $(COMPOSE_FILE) up -d dc1_node_1 second_cluster_dc1_node_1
	$(COMPOSE) exec -T --privileged dc1_node_1 su root -c 'echo 1048579 > /proc/sys/fs/aio-max-nr'
	@echo "==> Waiting for dc1 node1"
	@cnt=0; until [ 1 -le $$($(NODETOOL) status | grep -c "UN") ]; do \
		cnt=$$(expr $$cnt + 1); \
		if [ $$cnt -eq 300 ]; then exit 1; fi; \
		echo -n "."; sleep 2; done; echo ""

	@until [ 1 -le $$($(SECOND_NODETOOL) status | grep -c "UN") ]; do echo -n "."; sleep 2; done ; echo ""
	@. ./.env && CURRENT_UID=$(CURRENT_UID) CURRENT_GID=$(CURRENT_GID) $(COMPOSE) -f docker-compose.yaml -f $(COMPOSE_FILE) up -d second_cluster_dc1_node_2

	@cnt=1; for node in dc1_node_2 dc1_node_3 dc2_node_1 dc2_node_2 dc2_node_3; do \
  		. ./.env && CURRENT_UID=$(CURRENT_UID) CURRENT_GID=$(CURRENT_GID) $(COMPOSE) -f docker-compose.yaml -f $(COMPOSE_FILE) up -d $$node; \
		cnt=$$(expr $$cnt + 1); \
		echo "==> Waiting node $$node number $$cnt"; \
		until [ $$cnt -le $$($(NODETOOL) status | grep -c "UN") ]; do echo -n "."; sleep 2; done ; echo ""; done

	@echo "==> Start ssh servers"
	@for node in dc1_node_1 dc1_node_2 dc1_node_3 dc2_node_1 dc2_node_2 dc2_node_3 second_cluster_dc1_node_1 second_cluster_dc1_node_2; do \
		$(COMPOSE) exec -T --privileged $$node su root -c '/usr/sbin/sshd'; done

	@. ./.env && CURRENT_UID=$(CURRENT_UID) CURRENT_GID=$(CURRENT_GID) $(COMPOSE) -f docker-compose.yaml -f $(COMPOSE_FILE) up -d
	@echo "==> Waiting for the rest of containers"
	@until [ 1 -le $$($(SM_NODETOOL) status | grep -c "UN") ]; do echo -n "."; sleep 2; done ; echo ""

	@./nodes_exec "rm /root/.cqlshrc || true"
	@./nodes_exec "mkdir -p /root/.cassandra" 
	@./nodes_cp "scylla/cqlshrc" "/root/.cassandra/cqlshrc"

	@echo "==> Adding Minio user"
	./minio/add_user.sh || true
	@echo "==> Initialising cluster"
	@$(CQLSH_NODE) $(PUBLIC_NET)11 -u cassandra -p cassandra -e "ALTER KEYSPACE system_auth WITH REPLICATION = {'class': 'NetworkTopologyStrategy', 'dc1': 3, 'dc2': 3}" || true
	@$(CQLSH_NODE) $(PUBLIC_NET)11 -u cassandra -p cassandra -e "ALTER KEYSPACE audit WITH REPLICATION = {'class': 'NetworkTopologyStrategy', 'dc1': 3, 'dc2': 3}" || true

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
ifeq ($(IP_FAMILY), IPV6)
	@(./nodes_cp minio/certs_ipv6/CAs/rootCA.pem /usr/local/share/ca-certificates/rootCA.crt) || (./nodes_cp minio/certs_ipv6/CAs/rootCA.pem /etc/pki/ca-trust/source/anchors/rootCA.crt)
else
	@(./nodes_cp minio/certs_ipv4/CAs/rootCA.pem /usr/local/share/ca-certificates/rootCA.crt) || (./nodes_cp minio/certs_ipv4/CAs/rootCA.pem /etc/pki/ca-trust/source/anchors/rootCA.crt)
endif
	@(./nodes_exec "sudo update-ca-certificates") || (./nodes_exec "su root -c update-ca-trust")

.PHONY: restart-agent
restart-agent: ## Restart agent on all nodes
	@./nodes_exec supervisorctl restart scylla-manager-agent

.PHONY: help
help:
	@awk -F ':|##' '/^[^\t].+?:.*?##/ {printf "\033[36m%-25s\033[0m %s\n", $$1, $$NF}' $(MAKEFILE_LIST)
