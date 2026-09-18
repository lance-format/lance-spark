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

# Spark tarball versions
include docker/versions.mk
SPARK_DOWNLOAD_VERSION := $(SPARK_DOWNLOAD_VERSION_$(SPARK_VERSION))

# Spark 3.x default binaries are Scala 2.12; Scala 2.13 needs explicit suffix.
# Spark 4.x only supports Scala 2.13, so no suffix is needed.
ifeq ($(SCALA_VERSION),2.13)
  ifeq ($(filter 4.%,$(SPARK_VERSION)),)
    SPARK_SCALA_SUFFIX := -scala2.13
  else
    SPARK_SCALA_SUFFIX :=
  endif
else
  SPARK_SCALA_SUFFIX :=
endif

LANCE_NAMESPACE_IMPL_VERSION ?= $(shell sed -n 's:.*<lance-namespace-impl.version>\(.*\)</lance-namespace-impl.version>.*:\1:p' pom.xml | head -n 1)
INTEGRATION_PYTEST_CMD ?= pytest integration-tests/ -v --timeout=180

SPARK_DIST_TGZ := spark-$(SPARK_DOWNLOAD_VERSION)-bin-hadoop3$(SPARK_SCALA_SUFFIX).tgz
SPARK_DIST_URL := https://archive.apache.org/dist/spark/spark-$(SPARK_DOWNLOAD_VERSION)/$(SPARK_DIST_TGZ)
SPARK_CACHE_DIR := docker/.spark-cache
SPARK_CACHED_TGZ := $(SPARK_CACHE_DIR)/$(SPARK_DIST_TGZ)

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

.PHONY: clean-bundle
clean-bundle:
	./mvnw clean install -pl $(BUNDLE_MODULE) -am -DskipTests

.PHONY: install-base
install-base:
	./mvnw install -pl $(BASE_MODULE) -am -DskipTests

.PHONY: integration-test
integration-test: bundle
	SPARK_VERSION=$(SPARK_VERSION) \
	SCALA_VERSION=$(SCALA_VERSION) \
	SPARK_DOWNLOAD_VERSION=$(SPARK_DOWNLOAD_VERSION) \
	SPARK_SCALA_SUFFIX=$(SPARK_SCALA_SUFFIX) \
	SPARK_DIST_TGZ=$(SPARK_DIST_TGZ) \
	BUNDLE_MODULE=$(BUNDLE_MODULE) \
	LANCE_NAMESPACE_IMPL_VERSION=$(LANCE_NAMESPACE_IMPL_VERSION) \
	INTEGRATION_PYTEST_CMD="$(INTEGRATION_PYTEST_CMD)" \
	./scripts/run-integration-tests.sh

.PHONY: print-spark-test-args
print-spark-test-args:
	@echo "spark-download-version=$(SPARK_DOWNLOAD_VERSION)"
	@echo "spark-dist-tgz=$(SPARK_DIST_TGZ)"
	@echo "spark-scala-suffix=$(SPARK_SCALA_SUFFIX)"
	@echo "needs-spark-dist=$(if $(SPARK_SCALA_SUFFIX),true,false)"
	@echo "lance-namespace-impl-version=$(LANCE_NAMESPACE_IMPL_VERSION)"

.PHONY: fetch-spark-dist
fetch-spark-dist:
	@test -n "$(SPARK_DOWNLOAD_VERSION)" || { echo "unknown Spark download for SPARK_VERSION=$(SPARK_VERSION)"; exit 1; }
	mkdir -p $(SPARK_CACHE_DIR)
	@if [ ! -f "$(SPARK_CACHED_TGZ)" ] || ! tar tzf "$(SPARK_CACHED_TGZ)" >/dev/null 2>&1; then \
		echo "Downloading $(SPARK_DIST_URL)"; \
		rm -f "$(SPARK_CACHED_TGZ)" "$(SPARK_CACHED_TGZ).tmp"; \
		curl -fL --retry 3 --retry-delay 5 -o "$(SPARK_CACHED_TGZ).tmp" "$(SPARK_DIST_URL)" && \
		tar tzf "$(SPARK_CACHED_TGZ).tmp" >/dev/null && \
		mv "$(SPARK_CACHED_TGZ).tmp" "$(SPARK_CACHED_TGZ)"; \
	fi

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
	$(DOCKER_COMPOSE) -f docker/docker-compose.yml build --no-cache \
		--build-arg SPARK_DOWNLOAD_VERSION=$(SPARK_DOWNLOAD_VERSION) \
		--build-arg SPARK_MAJOR_VERSION=$(SPARK_VERSION) \
		--build-arg SCALA_VERSION=$(SCALA_VERSION) \
		--build-arg SPARK_SCALA_SUFFIX=$(SPARK_SCALA_SUFFIX) \
		spark-lance

.PHONY: docker-up
docker-up: check-docker-compose
	${DOCKER_COMPOSE} -f docker/docker-compose.yml up -d

.PHONY: docker-shell
docker-shell:
	cd docker && docker exec -it spark-lance bash

.PHONY: docker-down
docker-down: check-docker-compose
	${DOCKER_COMPOSE} -f docker/docker-compose.yml down

# =============================================================================
# Benchmark
# =============================================================================

.PHONY: benchmark-build
benchmark-build:
	cd benchmark && ../mvnw package -DskipTests \
		-Dspark.compat.version=$(SPARK_VERSION) \
		-Dscala.compat.version=$(SCALA_VERSION)

.PHONY: benchmark-generate
benchmark-generate:
	cd benchmark && \
		SPARK_VERSION=$(SPARK_VERSION) SCALA_VERSION=$(SCALA_VERSION) \
		./scripts/generate-data.sh $(SF) $(FORMATS) $(SPARK_MASTER)

.PHONY: benchmark-run
benchmark-run:
	cd benchmark && \
		SPARK_VERSION=$(SPARK_VERSION) SCALA_VERSION=$(SCALA_VERSION) \
		./scripts/run-benchmark.sh $(FORMATS) $(SPARK_MASTER) $(ITERATIONS)

.PHONY: benchmark
benchmark: benchmark-generate benchmark-run

.PHONY: benchmark-tpch-generate
benchmark-tpch-generate:
	cd benchmark && \
		SPARK_VERSION=$(SPARK_VERSION) SCALA_VERSION=$(SCALA_VERSION) \
		./scripts/generate-tpch-data.sh $(SF) $(FORMATS) $(SPARK_MASTER)

.PHONY: benchmark-tpch-run
benchmark-tpch-run:
	cd benchmark && \
		SPARK_VERSION=$(SPARK_VERSION) SCALA_VERSION=$(SCALA_VERSION) \
		./scripts/run-tpch-benchmark.sh $(FORMATS) $(SPARK_MASTER) $(ITERATIONS)

.PHONY: benchmark-tpch
benchmark-tpch: benchmark-tpch-generate benchmark-tpch-run

SF ?= 1
FORMATS ?= lance,parquet
SPARK_MASTER ?= local[*]
ITERATIONS ?= 3

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
	@echo "  bundle         - Build bundle module (incremental)"
	@echo "  clean-bundle   - Clean then build bundle module (use when source changes are not picked up)"
	@echo "  install-base   - Install base module"
	@echo "  integration-test - Run PySpark pytest"
	@echo "  fetch-spark-dist - Download Spark tarball for 3.x Scala 2.13 pytest"
	@echo ""
	@echo "Global commands (all modules):"
	@echo "  lint           - Check code style (checkstyle + spotless)"
	@echo "  format         - Apply spotless formatting"
	@echo "  install-all    - Install all modules without tests"
	@echo "  test-all       - Run all tests"
	@echo "  build-all      - Lint and install all modules"
	@echo "  clean          - Clean all modules"
	@echo ""
	@echo "Notebook Docker commands:"
	@echo "  docker-build           - Build the notebook image"
	@echo "  docker-up              - Start docker containers"
	@echo "  docker-shell           - Open shell in spark-lance container"
	@echo "  docker-down            - Stop docker containers"
	@echo ""
	@echo "Benchmark:"
	@echo "  benchmark-build         - Build benchmark jar (shared by TPC-DS and TPC-H)"
	@echo "  benchmark-generate      - Generate TPC-DS data via Spark (SF=1 FORMATS=lance,parquet)"
	@echo "  benchmark-run           - Run TPC-DS queries (FORMATS=lance,parquet ITERATIONS=3)"
	@echo "  benchmark               - Generate TPC-DS data + run queries (end-to-end)"
	@echo "  benchmark-tpch-generate - Generate TPC-H data via Spark (SF=1 FORMATS=lance,parquet)"
	@echo "  benchmark-tpch-run      - Run TPC-H queries (FORMATS=lance,parquet ITERATIONS=3)"
	@echo "  benchmark-tpch          - Generate TPC-H data + run queries (end-to-end)"
	@echo ""
	@echo "Documentation:"
	@echo "  serve-docs     - Serve documentation locally"
