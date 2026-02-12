# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Version parameters (can be overridden from command line)
# Example: make install SPARK_VERSION=3.5 SCALA_VERSION=2.13
SPARK_VERSION ?= 3.5
SCALA_VERSION ?= 2.12

# Derived module names
MODULE := lance-spark-$(SPARK_VERSION)_$(SCALA_VERSION)
BUNDLE_MODULE := lance-spark-bundle-$(SPARK_VERSION)_$(SCALA_VERSION)
BASE_MODULE := lance-spark-base_$(SCALA_VERSION)

# Spark download versions for Docker
include docker/versions.mk
SPARK_DOWNLOAD_VERSION := $(SPARK_DOWNLOAD_VERSION_$(SPARK_VERSION))
PY4J_VERSION := $(PY4J_VERSION_$(SPARK_VERSION))

# Spark 3.x default binaries are Scala 2.12; Scala 2.13 needs explicit suffix.
# Spark 4.0 only supports Scala 2.13, so no suffix is needed.
ifeq ($(SCALA_VERSION),2.13)
  ifneq ($(SPARK_VERSION),4.0)
    SPARK_SCALA_SUFFIX := -scala2.13
  else
    SPARK_SCALA_SUFFIX :=
  endif
else
  SPARK_SCALA_SUFFIX :=
endif

# Optional Docker build cache flags (set in CI for layer caching)
# Example: make docker-build-test-full DOCKER_CACHE_FROM="type=gha" DOCKER_CACHE_TO="type=gha,mode=max"
DOCKER_CACHE_FROM ?=
DOCKER_CACHE_TO ?=

DOCKER_COMPOSE := $(shell \
	if docker compose version >/dev/null 2>&1; then \
		echo "docker compose"; \
	elif command -v docker-compose >/dev/null 2>&1; then \
		echo "docker-compose"; \
	else \
		echo ""; \
	fi)

# =============================================================================
# Parameterized commands (use SPARK_VERSION and SCALA_VERSION)
# =============================================================================

.PHONY: install
install:
	./mvnw install -pl $(MODULE) -am -DskipTests

.PHONY: test
test:
	./mvnw test -pl $(MODULE)

.PHONY: build
build: lint install

.PHONY: clean-module
clean-module:
	./mvnw clean -pl $(MODULE)

.PHONY: bundle
bundle:
	./mvnw install -pl $(BUNDLE_MODULE) -am -DskipTests

.PHONY: install-base
install-base:
	./mvnw install -pl $(BASE_MODULE) -am -DskipTests

# =============================================================================
# Global commands (all modules)
# =============================================================================

.PHONY: lint
lint:
	./mvnw checkstyle:check spotless:check

.PHONY: format
format:
	./mvnw spotless:apply

.PHONY: install-all
install-all:
	./mvnw install -DskipTests

.PHONY: test-all
test-all:
	./mvnw test

.PHONY: build-all
build-all: lint install-all

.PHONY: clean
clean:
	./mvnw clean

# =============================================================================
# Docker commands
# =============================================================================

.PHONY: check-docker-compose
  check-docker-compose:
  ifndef DOCKER_COMPOSE
        $(error Neither 'docker compose' nor 'docker-compose' found. Please install Docker Compose.)
  endif

.PHONY: docker-build
docker-build:
	@ls $(BUNDLE_MODULE)/target/$(BUNDLE_MODULE)-*.jar >/dev/null 2>&1 || \
		(echo "Error: Bundle jar not found. Run 'make bundle' first." && exit 1)
	rm -f docker/lance-spark-bundle-*.jar
	cp $(BUNDLE_MODULE)/target/$(BUNDLE_MODULE)-*.jar docker/
	cd docker && $(DOCKER_COMPOSE) build --no-cache \
		--build-arg SPARK_DOWNLOAD_VERSION=$(SPARK_DOWNLOAD_VERSION) \
		--build-arg SPARK_MAJOR_VERSION=$(SPARK_VERSION) \
		--build-arg SCALA_VERSION=$(SCALA_VERSION) \
		--build-arg SPARK_SCALA_SUFFIX=$(SPARK_SCALA_SUFFIX) \
		spark-lance

.PHONY: docker-up
docker-up: check-docker-compose
	cd docker && ${DOCKER_COMPOSE} up -d

.PHONY: docker-shell
docker-shell:
	cd docker && docker exec -it spark-lance bash

.PHONY: docker-down
docker-down: check-docker-compose
	cd docker && ${DOCKER_COMPOSE} down

.PHONY: docker-build-test-base
docker-build-test-base:
	cd docker && docker build \
		--build-arg SPARK_DOWNLOAD_VERSION=$(SPARK_DOWNLOAD_VERSION) \
		--build-arg SPARK_MAJOR_VERSION=$(SPARK_VERSION) \
		--build-arg SCALA_VERSION=$(SCALA_VERSION) \
		--build-arg PY4J_VERSION=$(PY4J_VERSION) \
		--build-arg SPARK_SCALA_SUFFIX=$(SPARK_SCALA_SUFFIX) \
		-f Dockerfile.test-base \
		-t ghcr.io/hamersaw/lance-spark-test-base:$(SPARK_VERSION)_$(SCALA_VERSION) \
		.

.PHONY: docker-build-test
docker-build-test:
	@ls $(BUNDLE_MODULE)/target/$(BUNDLE_MODULE)-*.jar >/dev/null 2>&1 || \
		(echo "Error: Bundle jar not found. Run 'make bundle' first." && exit 1)
	rm -f docker/lance-spark-bundle-*.jar
	cp $(BUNDLE_MODULE)/target/$(BUNDLE_MODULE)-*.jar docker/
	cd docker && docker build --no-cache \
		--build-arg SPARK_MAJOR_VERSION=$(SPARK_VERSION) \
		--build-arg SCALA_VERSION=$(SCALA_VERSION) \
		-f Dockerfile.test \
		-t lance-spark-test:$(SPARK_VERSION)_$(SCALA_VERSION) \
		.

.PHONY: docker-build-test-full
docker-build-test-full:
	@ls $(BUNDLE_MODULE)/target/$(BUNDLE_MODULE)-*.jar >/dev/null 2>&1 || \
		(echo "Error: Bundle jar not found. Run 'make bundle' first." && exit 1)
	rm -f docker/lance-spark-bundle-*.jar
	cp $(BUNDLE_MODULE)/target/$(BUNDLE_MODULE)-*.jar docker/
	cd docker && docker build \
		--build-arg SPARK_DOWNLOAD_VERSION=$(SPARK_DOWNLOAD_VERSION) \
		--build-arg SPARK_MAJOR_VERSION=$(SPARK_VERSION) \
		--build-arg SCALA_VERSION=$(SCALA_VERSION) \
		--build-arg PY4J_VERSION=$(PY4J_VERSION) \
		--build-arg SPARK_SCALA_SUFFIX=$(SPARK_SCALA_SUFFIX) \
		-f Dockerfile.test-full \
		-t lance-spark-test:$(SPARK_VERSION)_$(SCALA_VERSION) \
		.

.PHONY: docker-test
docker-test:
	@docker image inspect lance-spark-test:$(SPARK_VERSION)_$(SCALA_VERSION) >/dev/null 2>&1 || \
		(echo "Error: Docker image 'lance-spark-test:$(SPARK_VERSION)_$(SCALA_VERSION)' not found. Run 'make docker-build-test' first." && exit 1)
	docker run --rm --hostname lance-spark \
		-e SPARK_VERSION=$(SPARK_VERSION) \
		lance-spark-test:$(SPARK_VERSION)_$(SCALA_VERSION) \
		"pytest /home/lance/tests/ -v --timeout=180"

# =============================================================================
# Documentation
# =============================================================================

.PHONY: serve-docs
serve-docs:
	cd docs && uv pip install -r requirements.txt && uv run mkdocs serve

# =============================================================================
# Help
# =============================================================================

.PHONY: help
help:
	@echo "Lance Spark Makefile"
	@echo ""
	@echo "Version parameters (defaults: SPARK_VERSION=3.5, SCALA_VERSION=2.12):"
	@echo "  Example: make install SPARK_VERSION=3.4 SCALA_VERSION=2.13"
	@echo ""
	@echo "Parameterized commands (use SPARK_VERSION and SCALA_VERSION):"
	@echo "  install        - Install module without tests"
	@echo "  test           - Run tests for module"
	@echo "  build          - Lint and install module"
	@echo "  clean-module   - Clean module"
	@echo "  bundle         - Build bundle module"
	@echo "  install-base   - Install base module"
	@echo ""
	@echo "Global commands (all modules):"
	@echo "  lint           - Check code style (checkstyle + spotless)"
	@echo "  format         - Apply spotless formatting"
	@echo "  install-all    - Install all modules without tests"
	@echo "  test-all       - Run all tests"
	@echo "  build-all      - Lint and install all modules"
	@echo "  clean          - Clean all modules"
	@echo ""
	@echo "Docker commands:"
	@echo "  docker-build           - Build docker image with Spark 3.5/Scala 2.12 bundle"
	@echo "  docker-up              - Start docker containers"
	@echo "  docker-shell           - Open shell in spark-lance container"
	@echo "  docker-down            - Stop docker containers"
	@echo "  docker-build-test-full - Build test image (with Spark and bundle)"
	@echo "  docker-test            - Run integration tests in lance-spark-test container"
	@echo ""
	@echo "Documentation:"
	@echo "  serve-docs     - Serve documentation locally"
