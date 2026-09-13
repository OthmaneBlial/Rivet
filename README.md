# Rivet

Rivet is a Rust-first CI/CD automation platform being built independently
from Jenkins.

## Delivery progress

**49% verified** · `█████████░░░░░░░░░░░`<br>
Measured against the weighted product scope in [ROADMAP.md](ROADMAP.md),
not against a claim of Jenkins feature parity. The percentage only counts
behavior backed by current tests or an exercised local workflow; incomplete
and unverified work remains at zero until it passes its gate.

Last verified update: **2026-09-13** · native pipeline execution, FIFO queue,
SQLite history, CLI workflow, headless API, Tauri desktop/logo, and Git/SCM
inspection, persisted build-source identity, live queue telemetry, durable
event replay, quiet engine offline recovery, explicit Git preparation at build
admission, parameterized builds, local artifact storage, and protected server
transport milestone.

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

The versioned API currently exposes health, projects, queued builds, build
details, persisted logs, cancellation, a live queue snapshot, durable replay,
and a per-build WebSocket event stream under `/api/v1/`. It also exposes Git repository inspection and an explicit
prepare operation for fetch/checkout/clean workflows. The server returns
`202 Accepted` when a build is queued. A build request may opt into Git
fetching, revision checkout, and workspace cleaning; the default remains
inspection-only. Clients read durable state from the build resource and
subscribe to live events separately.

The CLI exposes the same explicit SCM boundary, for example:

```sh
cargo run -p rivet -- run rivet --fetch --revision main --clean
```

Cleaning is never implicit.

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
```

Build parameters and local artifacts are also explicit:

```toml
[[parameters]]
name = "TARGET"
default = "debug"

[[artifacts]]
name = "bundle"
paths = ["dist/**"]
```

Parameters are resolved and persisted per build, then exposed to direct
processes as environment variables. Artifact files stay inside the pipeline
workspace, are copied to local Rivet storage with a SHA-256 checksum, and are
available through the build artifacts API or `rivet artifacts`.

Shell parsing is not implicit. A later pipeline feature may add an explicit
shell step with a documented threat boundary; direct process execution is the
safe default.

Inspect the source state behind a project with direct Git arguments:

```sh
cargo run -p rivet -- scm inspect .
cargo run -p rivet -- scm prepare . --revision main --clean
```

`prepare --clean` is intentionally opt-in because it removes untracked files.
