include .env

all: scylla-manager-agent scylla-manager ## Build all images

.PHONY: clean
clean: ## Remove images from the local repository
	docker image rm -f "scylladb/scylla-manager-agent:${SCYLLA_MANAGER_VERSION}" "scylladb/scylla-manager:${SCYLLA_MANAGER_VERSION}"

.PHONY: scylla-manager-agent
scylla-manager-agent: ## Build Scylla Manager Agent docker image
	@echo "==> Building Scylla Manager Agent docker image..."
	docker build scylla-manager-agent -t "scylladb/scylla-manager-agent:${SCYLLA_MANAGER_VERSION}"

.PHONY: scylla-manager
scylla-manager: ## Build Scylla Manager docker image
	@echo "==> Building Scylla Manager docker image..."
	docker build scylla-manager -t "scylladb/scylla-manager:${SCYLLA_MANAGER_VERSION}"

.PHONY: help
help:
	@awk -F ':|##' '/^[^\t].+?:.*?##/ {printf "\033[36m%-25s\033[0m %s\n", $$1, $$NF}' $(MAKEFILE_LIST)
