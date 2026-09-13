# Rivet delivery roadmap

This roadmap defines the denominator for the progress percentage shown in the
README. It is a weighted product plan for a complete Rivet platform; it is not
a Jenkins parity claim. A workstream advances only when its stated evidence is
present in the current repository and the relevant checks pass.

The current verified total is **88 / 100 points** (**88.43 weighted evidence
points**, displayed conservatively as 88%).

| Workstream | Weight | Current state | Gate for completion |
| --- | ---: | --- | --- |
| Foundation and domain contracts | 4 | verified | Rust workspace, IDs, explicit state transitions, tests |
| Pipeline definition and validation | 4 | verified | Versioned `Rivetfile.toml`, direct command arrays, validation tests |
| Native process runner | 6 | verified | stdout/stderr streaming, timeout, cancellation, process-group cleanup tests |
| Queue and scheduler | 6 | partial | FIFO tie ordering, bounded priorities, global/per-project limits, live telemetry, priority-ordered queue snapshots with a rendered queue control room, pre-execution cancellation, and authenticated pause/resume administration pass; resource requirements remain |
| SQLite persistence | 6 | verified | Migration, build graph projection, reopen/history test |
| API and live transport | 6 | partial | REST/loopback WebSocket, durable event replay, queue telemetry, build retry, schedule endpoints/dispatch, graceful SIGINT/SIGTERM shutdown, safe request IDs with structured method/route/status tracing, exact CORS allow-list, body limits, security headers, and policy-backed identity/project authorization pass; user sessions and deployment operations remain |
| CLI operator workflow | 3 | partial | project/run/history, parameterized builds, artifact listing, retry, and schedule management work; richer cancellation and inspection remain |
| Tauri desktop control room | 10 | partial | native bundle, embedded engine health, rendered light-default/dark-toggle UX, artifact/retry views, schedule controls, desktop SCM preparation controls, credential lifecycle view, online/offline recovery, real client build flow, and an ephemeral loopback engine origin in the packaged launch pass; final Tauri-window interaction and release QA remain |
| Git/SCM integration | 8 | partial | direct Git inspect/checkout/fetch/clean operations, bounded provider refspec fetches, REST/CLI surfaces, explicit build-admission preparation, persisted build source identity, provider-neutral credential IDs, ephemeral Git auth handoff, and provider push/PR adapters pass; credential/project lifecycle remains |
| Triggers and scheduling | 5 | partial | manual/API entry points, validated persistent UTC cron schedules/server dispatch, signed generic webhook delivery, and signed GitHub/GitLab push plus PR/MR adapters pass; broader repository-event and upstream triggers remain |
| Remote agents | 10 | partial | authenticated versioned registration/heartbeat contract, explicit pipeline requirements with local safety refusal, reconnecting agent CLI, reconnect-safe sessions, stale detection, capacity-aware online matching, rendered fleet view, matching capacity reservation, bounded workspace and artifact transfer, shared-runner remote execution, typed event/output relay, cancellation, one bounded replacement-agent attempt with clean terminal failure, and idempotent startup reconciliation of persisted incomplete builds pass; resuming the same remote attempt after restart, exactly-once semantics, and richer retry policy remain |
| Container execution | 4 | partial | validated container declarations and bounded Docker command assembly pass; Docker runtime cleanup, timeout/cancellation, image policy, and artifact extraction remain unverified |
| Artifacts | 4 | partial | local and remote workspace collection, upload/download, bounded checksum-verified transfer, metadata, storage boundary, and explicit retention pruning for completed builds pass; remote object backends remain |
| CI cache | 3 | partial | validated exact project-scoped primary/fallback keys, safe relative paths, atomic local archive save/restore, corrupt-entry recovery, fallback selection, and bounded local age-ordered pruning pass; remote cache backends remain |
| Secrets and credentials | 6 | partial | secret parameters require explicit runtime values, persisted values/API responses are redacted, logs are masked, retries require fresh secret input, and a passphrase-encrypted local credential vault with private-file CLI setup plus admin-only list/rotate/remove API and audit events passes; credential ownership, keychain integration, and project access control remain |
| Authentication and authorization | 4 | partial | protected remote transport with private Bearer tokens, policy-file token digests, roles, project scopes, route authorization, agent-connect permission checks, local token creation/listing/revocation, optional RFC3339 token expiration enforcement, and bounded admin-only authentication audit history pass; users, sessions, and external identity providers remain |
| Extension protocol | 4 | partial | versioned manifest, WASM/subprocess kind model, declared permission vocabulary, bounded length-prefixed JSON framing, direct-argument subprocess host, bounded local catalog discovery/API, desktop extension model, and a root-confined subprocess lifecycle manager with per-request permission enforcement pass; WASM runtime and server/desktop lifecycle UI remain |
| Jenkins migration analyzer | 3 | partial | bounded Jenkinsfile analysis, headless API delivery, rendered desktop report, and valid drafts for deterministic quoted shell steps pass; full Groovy parsing, plugin semantics, and broad generated Rivetfile conversion remain |
| Differential compatibility harness | 2 | partial | bounded JSON snapshots, explicit semantic normalization/comparison, mismatch exit status, and a checked-in regression fixture pass; live Jenkins/Rivet capture adapters and a permanent live regression corpus remain |
| Release and operations | 2 | partial | local release gate, optimized CLI artifact, desktop web/native checks, and versioned SHA-256 manifest pass; signed installers, observability, upgrades, recovery, security, and load gates remain |

## Progress policy

The README percentage is updated whenever a milestone changes one of these
states. “Planned” is zero progress, “partial” receives only the portion backed
by evidence, and “verified” receives the full workstream weight. A local build,
scaffold, passing unrelated test, or interface alone does not complete a gate.

The next gates are the WASM runtime and lifecycle UI, the final Tauri-window
flow inside the packaged app, API
deployment hardening, resuming remote attempts across server restart and
richer retry policy, credential ownership/rotation/keychain integration, user
sessions and external identity providers, live compatibility capture adapters
and a permanent differential regression corpus, broader repository-event
mapping, upstream triggers, full Groovy/plugin migration
semantics, and generated Rivetfile conversion with fixture-backed migration
regressions.
Test clean checkout behavior
without exposing credentials to logs or persisted state.
