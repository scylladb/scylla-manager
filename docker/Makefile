include .env

all: scylla-manager-agent scylla-manager ## Build all images

.PHONY: clean
clean: ## Remove images from the local repository
	docker image rm -f "scylladb/scylla-manager-agent:${SCYLLA_MANAGER_VERSION}" "scylladb/scylla-manager:${SCYLLA_MANAGER_VERSION}"

.PHONY: scylla-manager-agent
scylla-manager-agent: ## Build Scylla Manager Agent docker image
	@echo "==> Building Scylla Manager Agent docker image..."
	docker build scylla-manager-agent --format docker -t "scylladb/scylla-manager-agent:${SCYLLA_MANAGER_VERSION}"

.PHONY: scylla-manager
scylla-manager: ## Build Scylla Manager docker image
	@echo "==> Building Scylla Manager docker image..."
	docker build scylla-manager --format docker -t "scylladb/scylla-manager:${SCYLLA_MANAGER_VERSION}"

README.md: README.in Dockerfile docker-compose.yaml
	cp README.in README.md
	sed -i -e '/COMPOSE_FILE/r docker-compose.yaml' README.md
	sed -i -e '/COMPOSE_FILE/d' README.md
	sed -i -e '/DOCKER_FILE/r Dockerfile' README.md
	sed -i -e '/DOCKER_FILE/d' README.md

.PHONY: help
help:
	@awk -F ':|##' '/^[^\t].+?:.*?##/ {printf "\033[36m%-25s\033[0m %s\n", $$1, $$NF}' $(MAKEFILE_LIST)
