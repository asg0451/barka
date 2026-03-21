.PHONY: build check jepsen localstack localstack-down

build:
	cargo build

check:
	cargo check

localstack:
	docker compose up -d --wait

localstack-down:
	docker compose down -v

jepsen: build
	cd jepsen/barka && CLASSPATH= lein run test \
		--barka-bin $(CURDIR)/target/debug/barka \
		--project-root $(CURDIR)
