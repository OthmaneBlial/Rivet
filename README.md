# Rivet

Rivet is a Rust-first CI/CD automation platform being built independently
from Jenkins. Jenkins is kept under `base/jenkins/` as a local behavioral and
architectural reference only; that directory is intentionally ignored by Git.

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
  rivet-storage/    SQLite persistence, migrations, and event projection
  rivet-cli/        local operator interface and first runnable slice
apps/desktop/       Tauri client (next vertical slice)
compat/             measured Jenkins/Rivet compatibility data
base/jenkins/       ignored local reference checkout
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
projection, and SQLite history. Press Ctrl-C during a running step to exercise
the cancellation path.

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

Shell parsing is not implicit. A later pipeline feature may add an explicit
shell step with a documented threat boundary; direct process execution is the
safe default.
