---
name: Bug report
about: Report a reproducible problem in Rivet
title: "bug: "
labels: [bug]
assignees: []
---

## What happened?

Describe the observed behavior and the behavior you expected.

## Reproduction

Provide the smallest `Rivetfile.toml`, command, or UI sequence that reproduces
the problem. Remove credentials, private keys, customer data, and real
hostnames before posting.

## Environment

- Rivet version/commit:
- Operating system and architecture:
- CLI, desktop, or server:
- Container runtime, if relevant:

## Local evidence

```text
Paste the relevant command output, logs, or a sanitized screenshot here.
```

List the exact local checks you ran, for example:

```sh
cargo test --workspace
./scripts/local-progress-check.sh
```

## Additional context

Mention whether the issue is working, experimental, or related to a documented
limitation in [ROADMAP.md](../../ROADMAP.md).
