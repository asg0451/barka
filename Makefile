.PHONY: build check jepsen

build:
	cargo build

check:
	cargo check

jepsen: build
	cd jepsen/barka && CLASSPATH= lein run test --barka-bin $(CURDIR)/target/debug/barka
