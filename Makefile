.PHONY: build check jepsen localstack localstack-down

# If inside a distrobox, run container commands on the host.
# Otherwise use podman or docker directly.
ifdef DISTROBOX_ENTER_PATH
  CONTAINER_RT := distrobox-host-exec podman
else ifneq (,$(shell command -v podman 2>/dev/null))
  CONTAINER_RT := podman
else
  CONTAINER_RT := docker
endif

build:
	cargo build

check:
	cargo check

localstack:
	@if curl -sf http://localhost:4566/_localstack/health >/dev/null 2>&1; then \
		echo "LocalStack already running"; \
	else \
		$(CONTAINER_RT) rm -f barka-localstack 2>/dev/null || true; \
		$(CONTAINER_RT) run -d --name barka-localstack \
			-p 4566:4566 \
			-e SERVICES=s3 \
			-e DEFAULT_REGION=us-east-1 \
			-e EAGER_SERVICE_LOADING=1 \
			docker.io/localstack/localstack; \
		echo "Waiting for LocalStack..."; \
		for i in $$(seq 1 30); do \
			curl -sf http://localhost:4566/_localstack/health >/dev/null 2>&1 && break; \
			sleep 1; \
		done; \
		echo "LocalStack ready on http://localhost:4566"; \
	fi

localstack-down:
	$(CONTAINER_RT) rm -f barka-localstack 2>/dev/null || true

jepsen: build localstack
	cd jepsen/barka && CLASSPATH= lein run test \
		--barka-bin $(CURDIR)/target/debug/barka

brew-install-lein:
	brew install leiningen
