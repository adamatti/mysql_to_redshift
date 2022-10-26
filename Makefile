DOCKER_TAG=importer:local

.DEFAULT_GOAL := help

.PHONY: help
help: ## show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

clean-hard: dc-rm
	@rm -rf dist
	@rm -rf node_modules
	@rm -rf redshift
	@rm -f *.log

clean:
	@rm -rf dist

build-only: ## build only (without cleanup)
	@yarn -s build

build: clean build-only ## build the project

run: ## run the project
	@yarn -s start

run-js-only: ## run js without build
	@node dist/index.js

run-js: build run-js-only ## build and run js

dc-up: ## start docker dependencies
	@docker-compose up -d

dc-stop: ## stop docker containers
	@docker-compose stop -t 0

dc-rm: dc-stop
	@docker-compose rm -fv

fresh: dc-rm## do a fresh run
	@rm -rf redshift
	@$(MAKE) dc-up

docker-build:
	@docker build -t $(DOCKER_TAG) .

docker-sh: docker-build
	@docker run -it --rm --entrypoint /bin/sh -w /app $(DOCKER_TAG)