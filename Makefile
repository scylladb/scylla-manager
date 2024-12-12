all: help

# Prevent invoking make with a package specific test without a constraining a
# package.
ifneq "$(filter pkg-%,$(MAKECMDGOALS))" ""
ifeq "$(PKG)" ""
$(error Please specify package name with PKG e.g. PKG=./pkg/scyllaclient)
endif
endif

# Default package
PKG := ./pkg/...

SHELL := /bin/bash
GIT_ROOT = $(shell git rev-parse --show-toplevel)
GOBIN ?= $(shell pwd)/bin
GOFILES = go list -f '{{range .GoFiles}}{{ $$.Dir }}/{{ . }} {{end}}{{range .TestGoFiles}}{{ $$.Dir }}/{{ . }} {{end}}' $(PKG)

SCYLLA_VERSION?=scylla:6.0.1
IP_FAMILY?=IPV4
RAFT_SCHEMA?=none
TABLETS?=enabled
# if true starts the scylla cluster with ssl only config
SSL_ENABLED?=false

MANAGER_CONFIG := testing/scylla-manager/scylla-manager.yaml
PUBLIC_NET := 192.168.200.
MINIO_ENDPOINT := https://192.168.200.99:9000
ifeq ($(IP_FAMILY), IPV6)
	MANAGER_CONFIG := testing/scylla-manager/scylla-manager-ipv6.yaml
	PUBLIC_NET := 2001:0DB9:200::
	MINIO_ENDPOINT := https://[2001:0DB9:200::99]:9000
endif

.PHONY: fmt
fmt: ## Format source code
	@$(GOBIN)/golangci-lint run -c .golangci-fmt.yml --fix $(PKG)

.PHONY: check
check: ## Perform static code analysis
check: .check-go-version .check-copyright .check-comments \
.check-log-capital-letter .check-timeutc .check-lint .check-vendor

.PHONY: .check-go-version
.check-go-version:
	@[[ "`go version`" =~ "`cat .go-version`" ]] || echo "[WARNING] Required Go version `cat .go-version` found `go version | grep -o -E '1\.[0-9\.]+'`"

.PHONY: .check-copyright
.check-copyright:
	@set -e; for f in `$(GOFILES)`; do \
		[[ $$f =~ /mock_.*_test\.go ]] || \
		[[ $$f =~ _gen\.go ]] || \
		[[ `head -n 1 $$f` =~ '// Copyright (C) ' ]] || [[ `head -n 1 $$f` =~ 'DO NOT EDIT' ]] || (echo $$f; false); \
	done

.PHONY: .check-comments
.check-comments:
	@set -e; for f in `$(GOFILES)`; do \
		[[ $$f =~ _string\.go ]] || \
		! e=`grep -Pzo '\n\n\s+//\s*[a-z].*' $$f` || \
		(echo $$f $$e; false); \
	done

.PHONY: .check-log-capital-letter
.check-log-capital-letter:
	@set -e; for f in `$(GOFILES)`; do \
		! e=`grep -n -E '\.(Error|Info|Debug)\(ctx, "[a-z]' $$f` || \
		(echo $$f $$e; false); \
	done

.PHONY: .check-timeutc
.check-timeutc:
	@set -e; for f in `$(GOFILES)`; do \
		[[ $$f =~ /timeutc/ ]] || \
		! e=`grep -n 'time.\(Now\|Parse(\|Since\)' $$f` || \
		(echo $$f $$e; false); \
	done

.PHONY: .check-lint
.check-lint:
	@$(GOBIN)/golangci-lint run $(PKG)

.PHONY: .check-vendor
.check-vendor:
	@e=`go mod verify` || (echo $$e; false)

.PHONY: test
test: ## Run unit and integration tests
test: unit-test integration-test

.PHONY: unit-test
unit-test: ## Run unit tests
	@echo "==> Running tests (race)"
	@go test -cover -race $(PKG)

.PHONY: integration-test
integration-test: ## Run integration tests
integration-test:
	@$(MAKE) pkg-integration-test PKG=./pkg/ping/cqlping
	@$(MAKE) pkg-integration-test PKG=./pkg/ping/dynamoping
	@$(MAKE) pkg-integration-test PKG=./pkg/scyllaclient
	@$(MAKE) pkg-integration-test PKG=./pkg/service/backup
	@$(MAKE) pkg-integration-test PKG=./pkg/service/restore
	@$(MAKE) pkg-integration-test PKG=./pkg/service/cluster
	@$(MAKE) pkg-integration-test PKG=./pkg/service/healthcheck
	@$(MAKE) pkg-integration-test PKG=./pkg/service/repair
	@$(MAKE) pkg-integration-test PKG=./pkg/service/scheduler
	@$(MAKE) pkg-integration-test PKG=./pkg/store
	@$(MAKE) pkg-integration-test PKG=./pkg/schema/migrate

# Load Minio config for INTEGRATION_TEST_ARGS
include testing/.env

INTEGRATION_TEST_ARGS := -cluster $(PUBLIC_NET)100 \
-managed-cluster $(PUBLIC_NET)11,$(PUBLIC_NET)12,$(PUBLIC_NET)13,$(PUBLIC_NET)21,$(PUBLIC_NET)22,$(PUBLIC_NET)23 \
-test-network $(PUBLIC_NET) \
-managed-second-cluster $(PUBLIC_NET)31,$(PUBLIC_NET)32 \
-user cassandra -password cassandra \
-agent-auth-token token \
-s3-data-dir ./testing/minio/data -s3-provider Minio -s3-endpoint $(MINIO_ENDPOINT) -s3-access-key-id $(MINIO_USER_ACCESS_KEY) -s3-secret-access-key $(MINIO_USER_SECRET_KEY)

SSL_FLAGS := -ssl-ca-file ./testing/$(SSL_AUTHORITY_CRT) -ssl-key-file ./testing/$(SSL_CLIENT_KEY) -ssl-cert-file ./testing/$(SSL_CLIENT_CRT) -gocql.port $(SSL_PORT)

CURRENT_UID = $(shell id -u)
CURRENT_GID = $(shell id -g)

.PHONY: pkg-integration-test
pkg-integration-test: ## Run integration tests for a package, requires PKG parameter
pkg-integration-test: RUN=Integration
pkg-integration-test:
	@echo "==> Running integration tests for package $(PKG)"
	@docker kill scylla_manager_server 2> /dev/null || true
	@CGO_ENABLED=0 GOOS=linux go test -tags integration -c -o ./integration-test.dev $(PKG)
	@PWD=`pwd`; echo $(INTEGRATION_TEST_ARGS) $(SSL_FLAGS) $(ARGS) | sed -e "s=./testing=${PWD}/testing=g"
	@docker run --name "scylla_manager_server" \
		--network host \
		-v "/tmp:/tmp" \
		-v "$(PWD)/integration-test.dev:/usr/bin/integration-test:ro" \
		-v "$(PWD)/testing:/integration-test/testing" \
		-v "$(PWD)/$(PKG)/testdata:/integration-test/testdata" \
		-w "/integration-test" \
		-e "SSL_ENABLED=$(SSL_ENABLED)" \
		-u $(CURRENT_UID):$(CURRENT_GID) \
		-i --read-only --rm ubuntu integration-test -test.v -test.run $(RUN) $(INTEGRATION_TEST_ARGS) $(SSL_FLAGS) $(ARGS)

.PHONY: api-integration-test
api-integration-test: build-cli clean-server run-server
	@echo "==> Running API integration"
	@docker build -t scylla-manager-api-tests -f testing/api-tests.Dockerfile .
	@docker run --name scylla_manager_server_api_integration \
		--network host \
		-e "CGO_ENABLED=0" \
		-e "GOOD=linux" \
		-e "GOCACHE=/tmp/go-build" \
		-v "$(PWD):/scylla-manager" \
		-w "/scylla-manager" \
		-u $(CURRENT_UID):$(CURRENT_GID) \
		-i -d --rm scylla-manager-api-tests sleep infinity
	@docker exec -it scylla_manager_server_api_integration make build-cli OUTPUT=sctool.api-tests
	@docker exec -it scylla_manager_server_api_integration go test -tags api_integration ./pkg/command/... -test.v -test.run IntegrationAPI
	@$(MAKE) clean-server

.PHONY: pkg-stress-test
pkg-stress-test: ## Run unit tests for a package in parallel in a loop to detect sporadic failures, requires PKG parameter
pkg-stress-test: RUN=Test
pkg-stress-test:
	@echo "==> Running stress tests for package $(PKG) (race)"
	@go test -race -c -o stress.test $(PKG)
	@cd $(PKG); $(GOBIN)/stress $(PWD)/stress.test -test.run $(RUN)

.PHONY: start-dev-env
start-dev-env: ## Start testing containers
start-dev-env: .testing-up deploy-agent build-cli

.PHONY: .testing-up
.testing-up:
	@IPV6=$(IPV6) SCYLLA_VERSION=$(SCYLLA_VERSION) RAFT_SCHEMA=$(RAFT_SCHEMA) TABLETS=$(TABLETS) SSL_ENABLED=$(SSL_ENABLED) make -C testing build down up

.PHONY: dev-env-status
dev-env-status:  ## Checks status of docker containers and cluster nodes
	@IPV6=$(IPV6) make -C testing status

.PHONY: build-agent
build-agent: ## Build development agent binary
	@echo "==> Building agent"
	@CGO_ENABLED=0 GOOS=linux go build -trimpath -mod=vendor -o ./scylla-manager-agent.dev ./pkg/cmd/agent

.PHONY: deploy-agent
deploy-agent: build-agent ## Deploy it to testing containers
	@echo "==> Deploying agent to testing containers"
	@IPV6=$(IPV6) make -C testing deploy-agent restart-agent

.PHONY: build-cli
build-cli: ## Build development cli binary
build-cli: OUTPUT=./sctool.dev
build-cli:
	@echo "==> Building sctool"
	@go build -trimpath -mod=vendor -o $(OUTPUT) ./pkg/cmd/sctool

.PHONY: build-server
build-server: ## Build development server
	@echo "==> Building scylla-manager"
	@CGO_ENABLED=0 GOOS=linux go build -trimpath -mod=vendor -o ./scylla-manager.dev ./pkg/cmd/scylla-manager

.PHONY: clean-server
clean-server: ## Remove development server container
	@echo "==> Removing (if exists) scylla_manager_server container"
	@docker rm -f scylla_manager_server 2> /dev/null || true
	@docker rm -f scylla_manager_server_api_integration 2> /dev/null || true

.PHONY: run-server
run-server: build-server ## Build and run development server
	@docker run --name "scylla_manager_server" \
		--network scylla_manager_second \
		-p "5080:5080" \
		-p "5443:5443" \
		-p "5090:5090" \
		-v "$(PWD)/scylla-manager.dev:/usr/bin/scylla-manager:ro" \
		-v "$(PWD)/sctool.dev:/usr/bin/sctool:ro" \
		-v "$(PWD)/$(MANAGER_CONFIG):/etc/scylla-manager/scylla-manager.yaml:ro" \
		-v "/tmp:/tmp" \
		-d --read-only --rm scylladb/scylla-manager-dev scylla-manager
	@docker network connect scylla_manager_public scylla_manager_server

.PHONY: build
build: build-cli build-agent build-server ## Build all project binaries

.PHONY: docs
docs: export TZ = UTC ## set UTC as the default timezone in generated docs
docs: build ## Update docs/source with generated code
	@rm -rf docs/source/sctool/partials
	@SCYLLA_MANAGER_CLUSTER="" ./sctool.dev doc -o docs/source/sctool/partials

.PHONY: filler-tool
filler-tool: ## Build "filler" tool
	@echo "==> Building filler"
	@go build -trimpath -mod=vendor ./pkg/tools/filler/cmd/filler

.PHONY: clean
clean: ## Remove dev build artifacts (*.dev files)
	@rm -rf agent.dev sctool.dev scylla-manager.dev stress.test \
	filler

.PHONY: mrproper
mrproper: ## Clean go caches
	@go clean -r -cache -testcache -modcache ./...

.PHONY: generate
generate:  ## Recreate autogenerated resources
	@go generate $(PKG)

.PHONY: vendor
vendor: ## Fix dependencies and make vendored copies
	@go mod tidy
	@go mod vendor

.PHONY: help
help:
	@awk -F ':|##' '/^[^\t].+?:.*?##/ {printf "\033[36m%-25s\033[0m %s\n", $$1, $$NF}' $(MAKEFILE_LIST)

.PHONY: extract_tokens_and_schema
extract_tokens_and_schema:
	@echo "Extracting tokens from .1-1-restore-poc/backup.tar.gz..."
	@mkdir -p extracted_data
	@tar -xzvf .1-1-restore-poc/backup.tar.gz -C extracted_data

	# Extract tokens from meta/cluster/<cluster_id>/dc/<dc_id>/node/<node_id>
	@echo "Finding archives in extracted_data/meta/cluster/*/dc/*/node/*..."
	@find extracted_data/meta/cluster/*/dc/*/node/* -type f -name "*.gz" | while read inner_archive; do \
		echo "Processing archive: $$inner_archive"; \
		dc=$$(echo "$$inner_archive" | awk -F'/' '{for (i=1; i<=NF; i++) if ($$i == "dc") print $$(i+1)}'); \
		node=$$(echo "$$inner_archive" | awk -F'/' '{for (i=1; i<=NF; i++) if ($$i == "node") print $$(i+1)}'); \
		echo "  Found DC: $$dc"; \
		echo "  Found Node: $$node"; \
		tmp_dir=$$(mktemp -d); \
		gunzip -c "$$inner_archive" > "$$tmp_dir/extracted.json"; \
		if [ -s "$$tmp_dir/extracted.json" ]; then \
			echo "    Extracted JSON file: $$tmp_dir/extracted.json"; \
			tokens=$$(jq -r '.tokens | join(",")' "$$tmp_dir/extracted.json"); \
			echo "    Extracted Tokens: $$tokens"; \
			output_file=".1-1-restore-poc/$$dc-$$node.tokens"; \
			echo "$$tokens" > "$$output_file"; \
			echo "    Tokens saved to $$output_file"; \
		else \
			echo "    No valid JSON file found in $$inner_archive"; \
		fi; \
		rm -rf "$$tmp_dir"; \
	done

	# Extract schema CQL from schema/cluster/<cluster_id>/<file>.gz
	@echo "Processing schema files in extracted_data/schema/cluster/..."
	@find extracted_data/schema/cluster/* -type f -name "*.gz" | while read schema_archive; do \
		echo "Processing schema archive: $$schema_archive"; \
		tmp_dir=$$(mktemp -d); \
		gunzip -c "$$schema_archive" > "$$tmp_dir/schema.json"; \
		if [ -s "$$tmp_dir/schema.json" ]; then \
			echo "    Extracted schema JSON file: $$tmp_dir/schema.json"; \
			cql_stmts=$$(jq -r '.[].cql_stmt' "$$tmp_dir/schema.json"); \
			output_schema=".1-1-restore-poc/schema.CQL"; \
			echo "$$cql_stmts" > "$$output_schema"; \
			echo "    CQL statements saved to $$output_schema"; \
		else \
			echo "    No valid JSON schema file found in $$schema_archive"; \
		fi; \
		rm -rf "$$tmp_dir"; \
	done

	@rm -rf extracted_data
	@echo "All extracted data has been removed."

tokens_dir := ./.1-1-restore-poc
restore_dir := ./testing/scylla

.PHONY: set_initial_tokens
set_initial_tokens:
	@echo "Creating mapping and setting initial tokens..."
	@echo "Tokens directory: $(tokens_dir)"
	@echo "Restore directory: $(restore_dir)"
	@if [ ! -d "$(tokens_dir)" ]; then echo "Tokens directory does not exist: $(tokens_dir)"; exit 1; fi
	@if [ ! -d "$(restore_dir)" ]; then echo "Restore directory does not exist: $(restore_dir)"; exit 1; fi
	@tokens_files=$$(ls $(tokens_dir)/*.tokens 2>/dev/null | sort); \
	restore_files=$$(ls $(restore_dir)/scylla-PoC-restore-*.yaml 2>/dev/null | sort); \
	if [ -z "$$tokens_files" ]; then echo "No tokens files found in $(tokens_dir)"; exit 1; fi; \
	if [ -z "$$restore_files" ]; then echo "No restore YAML files found in $(restore_dir)"; exit 1; fi; \
	echo "Found tokens files: $$tokens_files"; \
	echo "Found scylla.yaml config files: $$restore_files"; \
	for tokens_file in $$tokens_files; do \
		tokens_dc=$$(basename $$tokens_file | awk -F'-' '{print $$1}'); \
		tokens_node=$$(basename $$tokens_file | awk -F'-' '{print $$2}' | sed 's/.tokens//'); \
		restore_file=$$(echo "$$restore_files" | grep "scylla-PoC-restore-$$tokens_dc" | head -1); \
		restore_files=$$(echo "$$restore_files" | sed "s|$$restore_file||"); \
		tokens=$$(cat $$tokens_file); \
		if [ -n "$$restore_file" ]; then \
			echo "Mapping $$tokens_file -> $$restore_file"; \
			awk -v tokens="initial_token: '$$tokens'" '/^initial_token:/ {sub(/^initial_token:.*/, tokens); found=1} {print} END {if (!found) print tokens}' $$restore_file > $$restore_file.tmp && mv $$restore_file.tmp $$restore_file; \
		else \
			echo "No matching restore file found for $$tokens_file"; \
		fi; \
	done
	@echo "Mapping and token updates complete."

cql_file := .1-1-restore-poc/schema.CQL
docker_container := scylla_manager-dc1_node_1-1

.PHONY: recreate_schema
recreate_schema:
	@echo "Executing CQL statements from $(cql_file) on Docker container $(docker_container)..."
	@if [ ! -f "$(cql_file)" ]; then echo "CQL file not found: $(cql_file)"; exit 1; fi
	@docker exec -i $(docker_container) cqlsh 192.168.100.11 -u cassandra -p cassandra --request-timeout=60 < $(cql_file)
	@echo "CQL execution completed."

data_file := .1-1-restore-poc/backuptest_data.big_table.sorted.restored.csv
extract_data_to_csv:
	@echo "Extracting data from Scylla table..."
	cqlsh 192.168.100.11 -u cassandra -p cassandra -e "COPY backuptest_data.big_table TO 'backuptest_data.big_table.restored.csv' WITH HEADER = TRUE;"
	@echo "Sorting the extracted data..."
	sort -t, -k1 backuptest_data.big_table.restored.csv > .1-1-restore-poc/backuptest_data.big_table.restored.sorted.csv
	@echo "Removing unsorted CSV file..."
	rm backuptest_data.big_table.restored.csv
	@echo "Data extraction and sorting complete!"

compare_restored:
	@echo "Comparing the content of restored ks with the expected content"
	@if diff .1-1-restore-poc/backuptest_data.big_table.restored.sorted.csv .1-1-restore-poc/backuptest_data.big_table.sorted.csv > /dev/null; then \
		echo "Contents are the same"; \
	else \
		echo "Contents differ"; \
	fi

extract_sstables:
	@echo "Extracting archive .1-1-restore-poc/backup.tar.gz..."
	@mkdir -p extracted_data
	@tar -xzvf .1-1-restore-poc/backup.tar.gz -C extracted_data

	@echo "Copying keyspaces for all nodes, excluding system keyspaces..."
	@find extracted_data/sst/cluster/*/dc/*/node/*/keyspace/* -maxdepth 0 -type d | while read keyspace_dir; do \
		keyspace_name=$$(basename "$$keyspace_dir"); \
		if [[ "$$keyspace_name" != system* ]]; then \
			split1=$$(dirname "$$keyspace_dir"); \
			node_dir=$$(dirname "$$split1"); \
			node_name=$$(basename "$$node_dir"); \
			split2=$$(dirname "$$node_dir"); \
			dc_dir=$$(dirname "$$split2"); \
			dc_name=$$(basename "$$dc_dir"); \
			output_dir=".1-1-restore-poc/$$dc_name-$$node_name/keyspace/$$keyspace_name"; \
			echo "DEBUG: Processing keyspace $$keyspace_name"; \
			echo "DEBUG: Node directory: $$node_dir"; \
			echo "DEBUG: Node name: $$node_name"; \
			echo "DEBUG: Data center name: $$dc_name"; \
			echo "DEBUG: Output directory: $$output_dir"; \
			echo "Copying keyspace $$keyspace_name to $$output_dir..."; \
			mkdir -p "$$output_dir"; \
			cp -r "$$keyspace_dir/"* "$$output_dir/"; \
		else \
			echo "Skipping system keyspace: $$keyspace_name"; \
		fi; \
	done

	@rm -rf extracted_data
	@echo "Keyspace copying complete, and extracted data removed."

# Define variables
SRC_DIR := .1-1-restore-poc
DEST_DIR := testing/scylla/data

# Target to generate the mapping from dc-host-id to dcXnY
generate_mapping:
	@echo "Generating mapping..."
	@rm -f .mapping
	@dc_list=$$(for dc_host_dir in $(SRC_DIR)/*; do \
		if [ -d "$$dc_host_dir" ]; then \
			dc_host_name=$$(basename "$$dc_host_dir"); \
			dc=$$(echo "$$dc_host_name" | cut -d'-' -f1); \
			echo "$$dc"; \
		fi; \
	done | sort -u); \
	for dc in $$dc_list; do \
		echo "Processing DC $$dc"; \
		host_dirs=$$(find $(SRC_DIR) -mindepth 1 -maxdepth 1 -type d -name "$$dc-*" | sort); \
		seq=1; \
		for host_dir in $$host_dirs; do \
			dc_host_name=$$(basename "$$host_dir"); \
			host_id=$$(echo "$$dc_host_name" | cut -d'-' -f2-); \
			dc_host_id="$$dc-$$host_id"; \
			dest_dc_n="$$dc""n""$$seq"; \
			echo "$$dc_host_id=$$dest_dc_n" >> .mapping; \
			seq=$$((seq+1)); \
		done; \
	done

# Target to copy files from source to destination
.PHONY: copy_sstables
copy_sstables: generate_mapping
	@echo "Copying files..."
	@while IFS='=' read -r dc_host_id dest_dc_n; do \
		echo "Processing $$dc_host_id => $$dest_dc_n"; \
		for src_path in $(SRC_DIR)/$$dc_host_id/keyspace/*/table/*/*; do \
			if [ ! -d "$$src_path" ]; then \
				continue; \
			fi; \
			keyspace=$$(echo "$$src_path" | sed -E 's|.*/keyspace/([^/]+)/table/.*|\1|'); \
			table=$$(echo "$$src_path" | sed -E 's|.*/table/([^/]+)/.*|\1|'); \
			dest_path="$(DEST_DIR)/$$dest_dc_n/$$keyspace/$$table-*"; \
			dest_upload_dir=$$(find $$dest_path -type d -name upload 2>/dev/null); \
			if [ -z "$$dest_upload_dir" ]; then \
				echo "Warning: No destination upload directory found for $$dest_path"; \
				continue; \
			fi; \
			echo "Setting permissions on $$(dirname "$$dest_upload_dir")"; \
			sudo chmod -R 777 "$$(dirname "$$dest_upload_dir")"; \
			echo "Copying from $$src_path to $$dest_upload_dir"; \
			cp -r "$$src_path"/* "$$dest_upload_dir"/; \
		done; \
	done < .mapping

# Define the list of Docker containers
CONTAINERS := \
    scylla_manager-dc1_node_1-1 \
    scylla_manager-dc1_node_2-1 \
    scylla_manager-dc1_node_3-1 \
    scylla_manager-dc2_node_1-1 \
    scylla_manager-dc2_node_2-1 \
    scylla_manager-dc2_node_3-1

.PHONY: refresh_nodes
refresh_nodes:
	@echo "Refreshing nodes..."
	@for container in $(CONTAINERS); do \
		echo "Refreshing $$container"; \
		docker exec $$container nodetool refresh backuptest_data big_table; \
	done

.PHONY: query_data
query_data:
	@echo "Executing CQL query on 192.168.100.11..."
	@cqlsh 192.168.100.11 -u cassandra -p cassandra -e "SELECT count(*) FROM backuptest_data.big_table;"

.PHONY: clean_restore
clean_restore:
	@echo "Cleaning nodes scylla data folder"
	sudo rm -rf ./testing/scylla/data/dc1n1/*
	sudo rm -rf ./testing/scylla/data/dc1n2/*
	sudo rm -rf ./testing/scylla/data/dc1n3/*
	sudo rm -rf ./testing/scylla/data/dc2n1/*
	sudo rm -rf ./testing/scylla/data/dc2n2/*
	sudo rm -rf ./testing/scylla/data/dc2n3/*
	@echo "Cleaning up .1-1-restore-poc, keeping only backup.tar.gz and backuptest_data.big_table.sorted.csv"
	find .1-1-restore-poc -mindepth 1 ! -name "backup.tar.gz" ! -name "backuptest_data.big_table.sorted.csv" -exec rm -rf {} +
	git checkout -- testing/scylla/scylla-PoC-restore-dc1-n1.yaml
	git checkout -- testing/scylla/scylla-PoC-restore-dc1-n2.yaml
	git checkout -- testing/scylla/scylla-PoC-restore-dc1-n3.yaml
	git checkout -- testing/scylla/scylla-PoC-restore-dc2-n1.yaml
	git checkout -- testing/scylla/scylla-PoC-restore-dc2-n2.yaml
	git checkout -- testing/scylla/scylla-PoC-restore-dc2-n3.yaml
