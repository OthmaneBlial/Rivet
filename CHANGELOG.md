# Changelog

## Unreleased

- owner-guarded credential removal across the CLI, API, and desktop control
  room;
- GitHub issue, pull request, support, and community guidance templates for
  evidence-led contributions.

## 0.1.0-alpha — 2026-09-14

First public preview of Rivet, a Rust-native CI/CD control room for explicit
local and remote build pipelines.

### Highlights

- versioned TOML pipelines with dependency-aware stage execution;
- durable SQLite build history, typed events, live output, queue controls,
  priorities, cancellation, retry, artifacts, and local cache;
- Git source inspection and preparation, schedules, signed webhooks, upstream
  gates, and GitHub Actions/GitLab/Bitbucket completion mappings;
- authenticated remote agents with capacity matching, bounded transfers, and
  shared-runner execution;
- Tauri 2 desktop control room with light mode by default and persistent dark
  mode;
- encrypted provider credentials, scoped authorization, opaque sessions,
  bounded extensions, and a Jenkinsfile migration analyzer;
- local macOS release gate with checksums and a packaged loopback engine.

### Known limitations

This is an unsigned macOS alpha. Windows/Linux packages, notarization, broad
provider interoperability, external identity providers, richer container
runtime behavior, full Jenkins/Groovy/plugin semantics, and production-scale
operations remain future gates. See [ROADMAP.md](ROADMAP.md).

### Validation

The release was validated locally with the workspace tests, CLI and desktop
builds, Tauri bundle launch smoke, end-to-end CLI/SCM workflow, queue,
backup/restore, authentication, compatibility, and deployment-hardening
smokes. This repository intentionally does not use GitHub Actions.
