.PHONY: check format-check test docs desktop-build release

check: format-check test docs desktop-build

format-check:
	cargo fmt --all -- --check

test:
	cargo test --workspace

docs:
	./scripts/local-documentation-check.sh

desktop-build:
	cd apps/desktop && npm run build

release:
	./scripts/local-release-check.sh
