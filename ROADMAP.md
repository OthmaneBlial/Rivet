# Rivet delivery roadmap

This roadmap defines the denominator for the progress percentage shown in the
README. It is a weighted product plan for a complete Rivet platform; it is not
a Jenkins parity claim. A workstream advances only when its stated evidence is
present in the current repository and the relevant checks pass.

The current verified total is **92 / 100 points** (**92.64 weighted evidence
points**, displayed conservatively as 92%).

| Workstream | Weight | Current state | Gate for completion |
| --- | ---: | --- | --- |
| Foundation and domain contracts | 4 | verified | Rust workspace, IDs, explicit state transitions, tests |
| Pipeline definition and validation | 4 | verified | Versioned `Rivetfile.toml`, direct command arrays, validation tests, dependency-checked DAGs, and deterministic parameter-gated stages with explicit skipped outcomes |
| Native process runner | 6 | verified | stdout/stderr streaming, timeout, cancellation, process-group cleanup tests |
| Queue and scheduler | 6 | partial | FIFO tie ordering, bounded priorities, global/per-project limits, live telemetry, priority-ordered queue snapshots with a rendered queue control room, pre-execution cancellation, authenticated pause/resume administration, executor/CPU/memory-aware remote resource reservations, dependency-ordered stage graph admission, and parallel independent stage execution pass; richer local resource dimensions remain |
| SQLite persistence | 6 | verified | Migration, build graph projection, reopen/history test |
| API and live transport | 6 | partial | REST/loopback WebSocket, durable event replay, queue telemetry, build retry, schedule endpoints/dispatch, graceful SIGINT/SIGTERM shutdown, safe request IDs with structured method/route/status tracing, exact CORS allow-list, body limits, security headers, and policy-backed identity/project authorization pass; deployment operations remain |
| CLI operator workflow | 3 | verified | project/run/history, parameterized builds, artifact listing, retry, schedule management, detailed build inspection, and one-shot authenticated remote build cancellation command |
| Tauri desktop control room | 10 | partial | native bundle, embedded engine health, rendered light-default/dark-toggle UX, artifact/retry views, schedule controls, desktop SCM preparation and runtime parameter controls, credential lifecycle view, online/offline recovery, real client build flow, and an ephemeral loopback engine origin in the packaged launch pass; final Tauri-window interaction and release QA remain |
| Git/SCM integration | 8 | partial | direct Git inspect/checkout/fetch/clean operations, bounded provider refspec fetches, REST/CLI surfaces, explicit build-admission preparation, persisted build source identity, provider-neutral credential IDs, ephemeral HTTP/SSH auth handoff, project-scoped credential resolution, provider push/PR adapters, and strict deployment-controlled SSH host-key policy pass; broader provider lifecycle remains |
| Triggers and scheduling | 5 | partial | manual/API entry points, validated persistent UTC cron schedules/server dispatch, signed generic webhook delivery, and signed GitHub/GitLab push plus PR/MR adapters pass; broader repository-event and upstream triggers remain |
| Remote agents | 10 | verified | authenticated versioned registration/heartbeat contract, explicit pipeline requirements with local safety refusal, reconnecting agent CLI, reconnect-safe sessions, stale detection, capacity-aware online matching, explicit CPU/memory capability matching and reservation, rendered fleet view, matching capacity reservation, bounded workspace and artifact transfer, shared-runner remote execution, typed event/output relay, cancellation, bounded step retry policy, one bounded replacement-agent attempt with clean terminal failure, persisted event-redelivery idempotence, idempotent startup reconciliation of persisted incomplete builds, redacted durable remote-attempt metadata with same-build/plan startup redispatch, persisted replacement-agent selection and retry budget, bounded session-scoped reliable delivery with ACKs, duplicate suppression, timed retransmission, and durable attempt/sequence identities atomically projected with build events; local replay and conflict tests pass |
| Container execution | 4 | partial | validated image pull/network/workspace-volume declarations, bounded Docker command assembly, signal proxying, cleanup flags, and a local runtime-shim execution test pass; Docker/Podman daemon behavior, image policy enforcement, timeout/cancellation, and artifact extraction remain unverified |
| Artifacts | 4 | partial | local and remote workspace collection, upload/download, bounded checksum-verified transfer, metadata, storage boundary, and explicit retention pruning for completed builds pass; remote object backends remain |
| CI cache | 3 | partial | validated exact project-scoped primary/fallback keys, safe relative paths, atomic local archive save/restore, corrupt-entry recovery, fallback selection, and bounded local age-ordered pruning pass; remote cache backends remain |
| Secrets and credentials | 6 | partial | secret parameters require explicit runtime values, persisted values/API responses are redacted, logs are masked, retries require fresh secret input, and a passphrase-encrypted local credential vault with typed HTTP/SSH entries, private-file CLI setup, project allow-lists, admin-only list/rotate/remove API, audit events, scoped build/webhook resolution, and an OS-keychain-backed vault passphrase source passes locally; deployment-specific keychain policy and richer ownership workflows remain |
| Authentication and authorization | 4 | partial | protected remote transport with private Bearer tokens, policy-file token digests, roles, project scopes, route authorization, agent-connect permission checks, local token creation/listing/revocation, optional RFC3339 token expiration enforcement, bounded admin-only authentication audit history, and persisted opaque session creation/use/expiry/revocation pass; users and external identity providers remain |
| Extension protocol | 4 | verified | versioned manifest, WASM/subprocess kind model, declared permission vocabulary, bounded length-prefixed JSON framing, direct-argument subprocess host, bounded local catalog discovery/API, desktop extension model, root-confined subprocess lifecycle manager with per-request permission enforcement, administrator-only server/desktop subprocess lifecycle controls, capability-free WASM runtime with explicit JSON ABI, denied imports, bounded module/memory/output sizes, fuel metering, bounded read-only build/log/artifact host methods, persisted stage-scoped annotations with permission-checked host writes, and permission-checked real build triggering pass |
| Jenkins migration analyzer | 3 | partial | bounded Jenkinsfile analysis, headless API delivery, rendered desktop report, and valid drafts for deterministic quoted shell steps pass; full Groovy parsing, plugin semantics, and broad generated Rivetfile conversion remain |
| Differential compatibility harness | 2 | partial | bounded JSON snapshots, explicit semantic normalization/comparison, mismatch exit status, and a checked-in regression fixture pass; live Jenkins/Rivet capture adapters and a permanent live regression corpus remain |
| Release and operations | 2 | partial | local release gate, optimized CLI artifact, desktop web/native checks, and versioned SHA-256 manifest pass; signed installers, observability, upgrades, recovery, security, and load gates remain |

## Progress policy

The README percentage is updated whenever a milestone changes one of these
states. “Planned” is zero progress, “partial” receives only the portion backed
by evidence, and “verified” receives the full workstream weight. A local build,
scaffold, passing unrelated test, or interface alone does not complete a gate.

The next gates are the final Tauri-window
flow inside the packaged app, API
deployment hardening, broader provider lifecycle and deployment-specific keychain policy coverage, user
accounts and external identity providers, live compatibility capture adapters
and a permanent differential regression corpus, broader repository-event
mapping, upstream triggers, full Groovy/plugin migration
semantics, and generated Rivetfile conversion with fixture-backed migration
regressions.
Test clean checkout behavior
without exposing credentials to logs or persisted state.
