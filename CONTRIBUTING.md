# Contributing to Rivet

Thank you for helping make Rivet a clear, inspectable CI/CD engine. Focused
issues, tests, fixtures, protocol evidence, and documentation are all useful.

## Before you start

1. Read [ROADMAP.md](ROADMAP.md) and choose a concrete gate.
2. Search existing issues and keep the change scoped to one vertical slice.
3. Never include provider tokens, private keys, passwords, real hostnames, or
   customer repository data in commits, screenshots, fixtures, or issue posts.

## Local checks

```sh
cargo fmt --all -- --check
cargo test --workspace
(cd apps/desktop && npm install && npm run build)
./scripts/local-progress-check.sh
```

The same baseline is available as `make check`. Use `make release` for the
full macOS packaging and integration gate.

On macOS, run `./scripts/local-release-check.sh` when changing packaging,
desktop integration, release behavior, storage boundaries, or deployment
hardening. The repository intentionally validates locally and does not consume
GitHub Actions minutes.

## Pull requests

- Explain the user-visible behavior and the boundary that remains unverified.
- Include exact commands and their results.
- Add or update deterministic tests for behavior changes.
- Keep public documentation free of local machine paths and private data.
- Do not label a scaffold, UI-only path, or synthetic fixture as production
  interoperability.

Small, reviewable pull requests are easier to validate and land.
