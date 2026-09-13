# Rivet delivery roadmap

This roadmap defines the denominator for the progress percentage shown in the
README. It is a weighted product plan for a complete Rivet platform; it is not
a Jenkins parity claim. A workstream advances only when its stated evidence is
present in the current repository and the relevant checks pass.

The current verified total is **52 / 100 points**.

| Workstream | Weight | Current state | Gate for completion |
| --- | ---: | --- | --- |
| Foundation and domain contracts | 4 | verified | Rust workspace, IDs, explicit state transitions, tests |
| Pipeline definition and validation | 4 | verified | Versioned `Rivetfile.toml`, direct command arrays, validation tests |
| Native process runner | 6 | verified | stdout/stderr streaming, timeout, cancellation, process-group cleanup tests |
| Queue and scheduler | 6 | partial | FIFO, global/per-project limits, live telemetry, and pre-execution cancellation pass; priorities, resource requirements, and admin state remain |
| SQLite persistence | 6 | verified | Migration, build graph projection, reopen/history test |
| API and live transport | 6 | partial | REST/loopback WebSocket, durable event replay, queue telemetry, and build retry pass; auth and deployment hardening remain |
| CLI operator workflow | 3 | partial | project/run/history, parameterized builds, artifact listing, and retry work; richer cancellation and inspection remain |
| Tauri desktop control room | 10 | partial | native bundle, embedded engine health, rendered light-default/dark-toggle UX, online/offline recovery, and real client build flow pass; final Tauri-window flow remains |
| Git/SCM integration | 8 | partial | direct Git inspect/checkout/fetch/clean operations, REST/CLI surfaces, explicit build-admission preparation, and persisted build source identity pass; credentials and provider hooks remain |
| Triggers and scheduling | 5 | planned | manual, webhook, API, cron, upstream triggers with deterministic tests |
| Remote agents | 10 | planned | authenticated versioned protocol, heartbeat, assignment, reconnect, failure handling |
| Container execution | 4 | planned | isolated Docker mode with cleanup, timeout, cancellation, artifact extraction |
| Artifacts | 4 | partial | local workspace collection, upload/download, checksums, metadata, and a storage boundary pass; retention and remote backends remain |
| CI cache | 3 | planned | safe keys, fallback behavior, corruption regression tests |
| Secrets and credentials | 6 | planned | encryption/access boundary, masking, non-serialization, threat documentation |
| Authentication and authorization | 4 | partial | protected remote transport with private Bearer tokens passes; users, sessions, roles, and project permissions remain |
| Extension protocol | 4 | planned | versioned WASM/subprocess/protocol boundary and frontend extension model |
| Jenkins migration analyzer | 3 | planned | measured Jenkinsfile analysis with supported/partial/unsupported output |
| Differential compatibility harness | 2 | planned | normalized behavioral fixtures and permanent regressions |
| Release and operations | 2 | planned | packaging, observability, upgrades, recovery, security and load gates |

## Progress policy

The README percentage is updated whenever a milestone changes one of these
states. “Planned” is zero progress, “partial” receives only the portion backed
by evidence, and “verified” receives the full workstream weight. A local build,
scaffold, passing unrelated test, or interface alone does not complete a gate.

The next gates are the final Tauri-window flow inside the packaged app, API
deployment hardening, and the SCM credential/provider boundary: add provider
hooks and test clean checkout behavior without exposing credentials to logs or
persisted state.
