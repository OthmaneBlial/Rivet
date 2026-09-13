# Rivet delivery roadmap

This roadmap defines the denominator for the progress percentage shown in the
README. It is a weighted product plan for a complete Rivet platform; it is not
a Jenkins parity claim. A workstream advances only when its stated evidence is
present in the current repository and the relevant checks pass.

The current verified total is **83 / 100 points** (**83.93 weighted evidence
points**, displayed conservatively as 83%).

| Workstream | Weight | Current state | Gate for completion |
| --- | ---: | --- | --- |
| Foundation and domain contracts | 4 | verified | Rust workspace, IDs, explicit state transitions, tests |
| Pipeline definition and validation | 4 | verified | Versioned `Rivetfile.toml`, direct command arrays, validation tests |
| Native process runner | 6 | verified | stdout/stderr streaming, timeout, cancellation, process-group cleanup tests |
| Queue and scheduler | 6 | partial | FIFO, global/per-project limits, live telemetry, and pre-execution cancellation pass; priorities, resource requirements, and admin state remain |
| SQLite persistence | 6 | verified | Migration, build graph projection, reopen/history test |
| API and live transport | 6 | partial | REST/loopback WebSocket, durable event replay, queue telemetry, build retry, schedule endpoints/dispatch, exact CORS allow-list, body limits, security headers, and policy-backed identity/project authorization pass; user sessions and deployment operations remain |
| CLI operator workflow | 3 | partial | project/run/history, parameterized builds, artifact listing, retry, and schedule management work; richer cancellation and inspection remain |
| Tauri desktop control room | 10 | partial | native bundle, embedded engine health, rendered light-default/dark-toggle UX, artifact/retry views, schedule controls, online/offline recovery, real client build flow, and an ephemeral loopback engine origin in the packaged launch pass; final Tauri-window interaction and release QA remain |
| Git/SCM integration | 8 | partial | direct Git inspect/checkout/fetch/clean operations, REST/CLI surfaces, explicit build-admission preparation, persisted build source identity, provider-neutral credential IDs, ephemeral Git auth handoff, and provider push adapters pass; credential/project lifecycle remains |
| Triggers and scheduling | 5 | partial | manual/API entry points, validated persistent UTC cron schedules/server dispatch, signed generic webhook delivery, and signed GitHub/GitLab push adapters pass; pull-request, broader repository-event, and upstream triggers remain |
| Remote agents | 10 | partial | authenticated versioned registration/heartbeat contract, explicit pipeline requirements with local safety refusal, reconnecting agent CLI, reconnect-safe sessions, stale detection, capacity-aware online matching, rendered fleet view, matching capacity reservation, bounded workspace and artifact transfer, shared-runner remote execution, typed event/output relay, cancellation, one bounded replacement-agent attempt with clean terminal failure, and idempotent startup reconciliation of persisted incomplete builds pass; resuming the same remote attempt after restart, exactly-once semantics, and richer retry policy remain |
| Container execution | 4 | partial | validated container declarations and bounded Docker command assembly pass; Docker runtime cleanup, timeout/cancellation, image policy, and artifact extraction remain unverified |
| Artifacts | 4 | partial | local and remote workspace collection, upload/download, bounded checksum-verified transfer, metadata, and a storage boundary pass; retention and remote object backends remain |
| CI cache | 3 | partial | validated exact project-scoped keys, safe relative paths, atomic local archive save/restore, and corrupt-entry recovery pass; fallback keys, eviction, and remote cache backends remain |
| Secrets and credentials | 6 | partial | secret parameters require explicit runtime values, persisted values/API responses are redacted, logs are masked, retries require fresh secret input, and a passphrase-encrypted local credential vault with private-file CLI setup passes; credential ownership, rotation, keychain integration, and project access control remain |
| Authentication and authorization | 4 | partial | protected remote transport with private Bearer tokens, policy-file token digests, roles, project scopes, route authorization, agent-connect permission checks, local token creation/listing/revocation, and optional RFC3339 token expiration enforcement pass; users, sessions, audit history, and external identity providers remain |
| Extension protocol | 4 | partial | versioned manifest, WASM/subprocess kind model, declared permission vocabulary, bounded length-prefixed JSON framing, direct-argument subprocess host, bounded local catalog discovery/API, desktop extension model, and a root-confined subprocess lifecycle manager with per-request permission enforcement pass; WASM runtime and server/desktop lifecycle UI remain |
| Jenkins migration analyzer | 3 | partial | bounded Jenkinsfile analysis, headless API delivery, rendered desktop report, and valid drafts for deterministic quoted shell steps pass; full Groovy parsing, plugin semantics, and broad generated Rivetfile conversion remain |
| Differential compatibility harness | 2 | planned | normalized behavioral fixtures and permanent regressions |
| Release and operations | 2 | planned | packaging, observability, upgrades, recovery, security and load gates |

## Progress policy

The README percentage is updated whenever a milestone changes one of these
states. “Planned” is zero progress, “partial” receives only the portion backed
by evidence, and “verified” receives the full workstream weight. A local build,
scaffold, passing unrelated test, or interface alone does not complete a gate.

The next gates are extension permission enforcement in a lifecycle manager, the
WASM runtime and lifecycle UI, the final Tauri-window flow inside the packaged app, API
deployment hardening, resuming remote attempts across server restart and
richer retry policy, credential ownership/rotation/keychain integration, user
sessions and audit history, pull-request and broader
repository-event mapping, upstream triggers, full Groovy/plugin migration
semantics, and generated Rivetfile conversion with fixture-backed migration
regressions.
Test clean checkout behavior
without exposing credentials to logs or persisted state.
