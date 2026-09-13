# Rivet

Rivet is a Rust-first CI/CD automation platform being built independently
from Jenkins.

## Delivery progress

**75% verified** · `███████████████░░░░░`<br>
Weighted evidence score: **75.92 / 100** · displayed conservatively as the
whole-number floor<br>
Measured against the weighted product scope in [ROADMAP.md](ROADMAP.md),
not against a claim of Jenkins feature parity. The percentage only counts
behavior backed by current tests or an exercised local workflow; incomplete
and unverified work remains at zero until it passes its gate.

Last verified update: **2026-09-13** · native pipeline execution, FIFO queue,
SQLite history, CLI workflow, headless API, Tauri desktop/logo, and Git/SCM
inspection, persisted build-source identity, live queue telemetry, durable
event replay, quiet engine offline recovery, explicit Git preparation at build
admission, parameterized builds, local artifact storage, protected server
transport, build retry, pre-execution queue cancellation, build artifact
downloads, and a light-default desktop theme with an accessible dark-mode
toggle, persistent UTC cron schedules, server dispatch, desktop schedule
controls, signed generic webhook delivery with idempotent redelivery,
secret-parameter redaction/masking, and a packaged desktop launch with an
ephemeral loopback engine origin, project-scoped local CI cache restore and
save, explicit Docker container command assembly with bounded workspace mounts,
and a bounded Jenkinsfile migration analyzer with line-level support findings
plus safe drafts for deterministic shell steps, exposed through the headless
API and rendered in the desktop control room. The server also exposes a
versioned agent handshake/heartbeat registry with online/stale state, capacity-aware
matching, a reconnecting heartbeat CLI client, and a rendered fleet view. Pipeline
steps can declare exact remote requirements; the local runner refuses those steps
until assignment exists. A matching agent can now reserve capacity, receive a
bounded workspace archive, execute the assigned pipeline through the shared Rust
runner, and relay typed events, output, and cancellation. Remote artifact
bundles now return through a bounded, checksum-verified channel. If the assigned
agent disconnects, the server makes at most one replacement-agent attempt and
closes unfinished steps, stages, and builds as failed when recovery is
unavailable; durable recovery across a server restart and richer retry policy
remain future gates.

The project is being developed as working vertical slices. The current slice
defines a versioned TOML pipeline model with explicit executable/argument
arrays, validated repository-scoped workspaces, persisted domain-safe IDs, and
typed build events. It is not Jenkins parity and does not claim production
readiness yet.

## Workspace

```text
crates/
  rivet-core/       domain model, pipeline format, and event schema
  rivet-runner/     process execution, queue, and pipeline orchestration
  rivet-server/     headless REST/WebSocket transport
  rivet-storage/    SQLite persistence, migrations, and event projection
  rivet-cli/        local operator interface and first runnable slice
apps/desktop/       Tauri client (next vertical slice)
compat/             measured Jenkins/Rivet compatibility data
```

## Validate the current slice

```sh
cargo test --workspace
cargo fmt --all -- --check
```

Validation is intentionally local for this repository; there is no GitHub
Actions workflow to consume hosted CI minutes.

## Run a local build

From a repository containing `Rivetfile.toml`:

```sh
cargo run -p rivet -- init .
cargo run -p rivet -- project create rivet --repository .
cargo run -p rivet -- run rivet
cargo run -p rivet -- builds rivet
cargo run -p rivet -- logs rivet --build 1
```

The build command uses the Rust queue, real child processes, live event
projection, and SQLite history. For Git repositories, the build record also
captures the commit, reference, remote, and dirty state observed at admission.
Press Ctrl-C during a running step to exercise the cancellation path.

## Run headless

The same engine can run without the desktop client:

```sh
cargo run -p rivet -- --data-dir .rivet server --bind 127.0.0.1:7878
```

Loopback server mode is intended for the local desktop flow. A non-loopback
bind requires a private token file:

```sh
chmod 600 /secure/path/rivet.token
cargo run -p rivet -- --data-dir .rivet server \
  --bind 0.0.0.0:7878 --token-file /secure/path/rivet.token
```

The token is held in memory, never printed, and never stored in the Rivet
database. Health checks remain public; API and WebSocket routes require
`Authorization: Bearer <token>` when authentication is enabled.

Browser access uses an exact local/Tauri origin allow-list by default. Add an
exact remote console origin explicitly when needed; wildcard origins are
rejected:

```sh
cargo run -p rivet -- --data-dir .rivet server \
  --allow-origin https://console.example
```

The versioned API currently exposes health, projects, queued builds, build
details, persisted logs, cancellation, a live queue snapshot, durable replay,
and a per-build WebSocket event stream under `/api/v1/`. It also exposes
persisted UTC cron schedules with create/list/pause/resume/delete operations,
automatic server dispatch, Git repository inspection, and an explicit prepare
operation for fetch/checkout/clean workflows. The server returns
`202 Accepted` when a build is queued. A build request may opt into Git
fetching, revision checkout, and workspace cleaning; the default remains
inspection-only. Clients read durable state from the build resource and
subscribe to live events separately.

Generic webhook delivery is available at `POST /api/v1/webhooks/generic`. The
server verifies `X-Rivet-Signature: sha256=<hex HMAC-SHA256 of the raw body>`
before accepting this payload shape:

```json
{
  "event_id": "provider-delivery-123",
  "project": "rivet",
  "revision": "main",
  "remote": "origin",
  "fetch": true,
  "parameters": { "TARGET": "release" }
}
```

Configure the signing key through a private file; Rivet trims the file's final
newline, keeps the value in memory, and never stores or prints it:

```sh
chmod 600 /secure/path/rivet.webhook.secret
cargo run -p rivet -- --data-dir .rivet server \
  --webhook-secret-file /secure/path/rivet.webhook.secret
```

The same `event_id` can be retried safely: the first request queues one build,
and later deliveries return a deduplicated response without creating another
build. A non-loopback server still requires the separate Bearer token.

Remote agents use a versioned WebSocket contract at
`GET /api/v1/agents/connect`. Agents register capabilities such as operating
system, architecture, Docker availability, labels, and executor capacity,
then send monotone heartbeats. `GET /api/v1/agents` reports the current
ephemeral registry; silent agents become `stale` after the heartbeat window.
`POST /api/v1/agents/match` accepts exact capability requirements and excludes
stale or saturated agents. A build with a remote step reserves a matching online
agent, transfers the repository workspace in bounded chunks, executes it with
the shared Rust runner, and persists the agent's typed build events and output.
Cancellation is propagated to the agent, and declared artifacts return through
the same bounded transfer with checksum verification before local storage.
After an agent disconnect, the server makes one bounded replacement attempt and
persists a terminal failed state when no replacement is available. Durable
recovery across a server restart, exactly-once guarantees, and richer retry
policy remain future gates.

Connect a worker for heartbeat and capability discovery:

```sh
cargo run -p rivet -- agent \
  --server ws://127.0.0.1:7878/api/v1/agents/connect \
  --name linux-builder --os linux --arch x86_64 \
  --label build --executors 2
```

The command keeps its stable agent ID and reconnects with bounded backoff after a
transport interruption. It accepts assignments, stages each workspace under an
isolated build-specific directory, and uses the same Rust process runner as local
execution. `--workspace-root` can select the local parent directory; the default
is a temporary agent workspace. The transport is currently bounded to a 512 MiB
workspace archive and rejects unsafe archive entries.

The CLI exposes the same explicit SCM boundary, for example:

```sh
cargo run -p rivet -- run rivet --fetch --revision main --clean
```

Cleaning is never implicit.

Completed builds can be retried without losing their original history. The
retry creates a new build number and reuses the original non-secret resolved
parameters unless the API caller supplies replacements. Secret parameters must
be supplied again explicitly with `--param NAME=VALUE` on the CLI or in the
API request body.

Schedules can also be managed from the CLI. Expressions use UTC and accept
the familiar five-field form:

```sh
cargo run -p rivet -- schedule create rivet \
  --name nightly --expression "0 2 * * *"
cargo run -p rivet -- schedule list rivet
```

The first executable pipeline format is deliberately explicit:

```toml
version = 1
name = "sample"

[[stages]]
name = "Test"

[[stages.steps]]
name = "unit"
program = "cargo"
args = ["test"]
timeout_seconds = 300
[stages.steps.container]
image = "rust:1.85"
```

Build parameters and local artifacts are also explicit:

```toml
[[parameters]]
name = "TARGET"
default = "debug"

[[parameters]]
name = "DEPLOY_TOKEN"
secret = true

[[caches]]
name = "rust-target"
key = "rust-target-v1"
paths = ["target"]

[[artifacts]]
name = "bundle"
paths = ["dist/**"]
```

Parameters are resolved per build and exposed to direct processes as
environment variables. Non-secret values are persisted for history; secret
parameters cannot define defaults, are represented as `[redacted]` in stored
build data and API responses, and are replaced with `***` in emitted logs.
Cache paths use an exact project-scoped key, restore before the first stage, and
save only after a successful build to an atomic archive under Rivet's local
data directory; a missing or corrupt cache never fails the build. Artifact
files stay inside the pipeline workspace, are copied to local Rivet storage
with a SHA-256 checksum, and are available through the build artifacts API or
`rivet artifacts`. Remote agents package only declared artifact matches and
return them through bounded checksum-verified archive chunks before the build
is marked passed.

A step can opt into explicit Docker execution with `[stages.steps.container]`.
Rivet assembles a direct `docker run` invocation with a private workspace mount,
the validated working directory, separated arguments, and automatic container
cleanup. Docker runtime execution, image policy, and end-to-end artifact and
cancellation behavior remain unverified on machines without Docker.

Inspect a Jenkinsfile locally before attempting a migration, or request a
safe draft for simple quoted commands:

```sh
cargo run -p rivet -- analyze jenkinsfile --draft ./Jenkinsfile
```

The analyzer emits versioned JSON with supported, partial, and unsupported
constructs, source line numbers, and Rivet mapping guidance. The optional
draft emits a valid Rivetfile for simple, explicitly quoted `sh`/`bat` steps
and leaves ambiguous commands, credentials, plugins, and lifecycle behavior in
warnings. It never executes Groovy or plugin code; complex migration semantics
still need manual review.

Shell parsing is not implicit. A later pipeline feature may add an explicit
shell step with a documented threat boundary; direct process execution is the
safe default.

Inspect the source state behind a project with direct Git arguments:

```sh
cargo run -p rivet -- scm inspect .
cargo run -p rivet -- scm prepare . --revision main --clean
```

`prepare --clean` is intentionally opt-in because it removes untracked files.
