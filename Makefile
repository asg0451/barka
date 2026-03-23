.PHONY: build check test jepsen localstack localstack-down

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
	@CONTAINER_RT="$(CONTAINER_RT)" bash "$(CURDIR)/scripts/ensure-localstack.sh"
	@echo "LocalStack ready on http://localhost:4566"

test: localstack
	cargo test

localstack-down:
	$(CONTAINER_RT) rm -f barka-localstack 2>/dev/null || true

NODES ?= 1

jepsen: build localstack
	cd jepsen/barka && CLASSPATH= lein run test \
		--barka-bin $(CURDIR)/target/debug/barka \
		--num-nodes $(NODES)

brew-install-lein:
	brew install leiningen
