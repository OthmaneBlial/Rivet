# Rivet

Rivet is a Rust-first CI/CD automation platform being built independently
from Jenkins.

## Delivery progress

**87% verified** · `███████████████████░`<br>
Weighted evidence score: **87.83 / 100** · displayed conservatively as the
whole-number floor<br>
Measured against the weighted product scope in [ROADMAP.md](ROADMAP.md),
not against a claim of Jenkins feature parity. The percentage only counts
behavior backed by current tests or an exercised local workflow; incomplete
and unverified work remains at zero until it passes its gate.

Last verified update: **2026-09-13** · native pipeline execution, priority-aware
FIFO queue with pause/resume controls,
SQLite history, CLI workflow, headless API, Tauri desktop/logo, and Git/SCM
inspection, persisted build-source identity, live queue telemetry, durable
event replay, quiet engine offline recovery, explicit Git preparation at build
admission, parameterized builds, local artifact storage and retention pruning,
protected server
transport with safe request IDs and structured method/route/status tracing,
build retry, pre-execution queue cancellation, build artifact
downloads, and a light-default desktop theme with an accessible dark-mode
toggle, persistent UTC cron schedules, server dispatch, desktop schedule
controls, signed generic webhook delivery with idempotent redelivery,
policy-backed API identities with role/project authorization,
secret-parameter redaction/masking, a passphrase-encrypted SCM credential vault
with non-secret credential references, and a packaged desktop launch with an
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
unavailable. On startup, persisted incomplete builds are reconciled
idempotently so a crashed server cannot leave history stuck forever; resuming
the same remote attempt after restart and richer retry policy remain future
gates. Authenticated deployments persist bounded success/failure audit records
without request bodies or Bearer values and expose them only to administrators;
user sessions and external identity providers remain future gates.

The repository includes a local-only release gate. It runs the workspace tests,
builds the optimized CLI, builds the desktop web client, checks the native
Tauri host, copies the CLI into a temporary release directory, and writes a
versioned SHA-256 manifest:

```sh
./scripts/local-release-check.sh
```

This produces a locally verifiable artifact, not a signed installer, store
submission, or hosted CI result.

The headless server handles SIGINT/SIGTERM with a graceful HTTP shutdown and
stops its schedule dispatcher after the listener closes. Rivet's extension
surface is intentionally a separate, versioned contract.
`rivet-extension-protocol` validates WASM or direct-subprocess manifests,
declared permissions, relative entrypoints, and bounded length-prefixed JSON
frames. An optional local manifest directory is loaded with strict regular
file checks and duplicate-ID rejection, then exposed through
`GET /api/v1/extensions`; the desktop control room mirrors that model and
reports the validated catalog. The extension crate also provides a bounded
subprocess lifecycle manager: it resolves only regular executables below an
explicit root, rejects symlink/path escapes, permits one session per ID, and
checks the declared permission on every host request. WASM execution and
server/desktop lifecycle controls remain gated until their sandbox and UX are
implemented.

The project is being developed as working vertical slices. The current slice
defines a versioned TOML pipeline model with explicit executable/argument
arrays, validated repository-scoped workspaces, persisted domain-safe IDs, and
typed build events. It is not Jenkins parity and does not claim production
readiness yet.

## Workspace

```text
crates/
  rivet-core/        domain model, pipeline format, and event schema
  rivet-runner/      process execution, queue, and pipeline orchestration
  rivet-server/      headless REST/WebSocket transport
  rivet-storage/     SQLite persistence, migrations, and event projection
  rivet-extension-protocol/ bounded WASM/subprocess extension contract
  rivet-credentials/ encrypted local provider credentials
  rivet-scm/         direct Git adapter and SCM boundary
  rivet-cli/         local operator interface and first runnable slice
apps/desktop/       Tauri client (next vertical slice)
compat/             measured Jenkins/Rivet compatibility data
rivet-compat/       normalized local behavior comparison harness
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
cargo run -p rivet -- run rivet --priority 20
cargo run -p rivet -- builds rivet
cargo run -p rivet -- logs rivet --build 1
```

The build command uses the Rust queue, real child processes, live event
projection, and SQLite history. For Git repositories, the build record also
captures the commit, reference, remote, and dirty state observed at admission.
Use `--priority -100..100` to move urgent builds ahead of older queued work;
equal priorities retain FIFO order and per-project/global capacity limits still
apply. The HTTP build body accepts the same `priority` field. Press Ctrl-C
during a running step to exercise the cancellation path.

Administrators can stop new admissions without interrupting running builds with
`POST /api/v1/queue/pause`, inspect the `paused` field from `GET
/api/v1/queue`, and reopen admissions with `POST /api/v1/queue/resume`.

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

For a server deployment with multiple roles, use a private authentication
policy file. It stores only token digests, never raw Bearer tokens:

```json
{
  "version": 1,
  "tokens": [
    {
      "id": "operator",
      "sha256": "<64 lowercase hex characters>",
      "role": "operator",
      "projects": ["rivet"],
      "expires_at": "2026-12-31T23:59:59Z"
    }
  ]
}
```

Protect the file and pass it to the server with
`--auth-policy-file /secure/path/rivet.auth.json`. Supported roles are
`admin`, `operator`, `viewer`, and `agent`; project routes are filtered and
mutations require the corresponding role and scope. The legacy private
`--token-file` remains available for a single unrestricted deployment.
Operators can manage policy tokens locally without hand-computing digests. The
create command generates the token, stores it in a new private `0600` file, and
writes only its SHA-256 digest to the policy. Create a replacement before
revoking the old token during rotation:

```sh
cargo run -p rivet -- auth token create operator \
  --role operator --project rivet \
  --expires-at 2026-12-31T23:59:59Z \
  --policy-file /secure/path/rivet.auth.json \
  --token-file /secure/path/rivet.operator.token
cargo run -p rivet -- auth token list \
  --policy-file /secure/path/rivet.auth.json
cargo run -p rivet -- auth token revoke old-operator \
  --policy-file /secure/path/rivet.auth.json
```

The token value is not printed by the command and is not recoverable from the
policy. The optional RFC3339 `expires_at` value is enforced at request time;
legacy records without it remain non-expiring. Authenticated requests also
produce bounded audit records available to administrators at
`GET /api/v1/audit`; request bodies and Bearer values are never recorded. User
accounts, sessions, and external identity providers remain future gates.

Recorded Jenkins/Rivet behavior snapshots can be compared locally with the
same explicit normalizer used by future live adapters:

```sh
cargo run -p rivet -- compat compare ./compat/fixtures/sequential-build.json
```

The command exits non-zero when normalized stage, step, parameter, artifact, or
log semantics differ. The checked-in fixture exercises provider status spelling,
CRLF handling, checksum prefixes, and redaction markers; live Jenkins/Rivet
capture adapters and a permanent live regression corpus remain future gates.

Extension manifests can be loaded by the headless server from an explicit
local directory:

```sh
cargo run -p rivet -- --data-dir .rivet server \
  --extension-manifest-dir /secure/path/rivet-extensions
```

Only `.json` regular files are considered. Manifests are validated for
protocol version, relative entrypoint, unique ID, and declared permissions;
the catalog does not execute or auto-grant an extension.

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

Provider webhook adapters are available when their provider secret is supplied.
The project name is part of the route, so the receiver never guesses a Rivet
project from an untrusted repository name:

```text
POST /api/v1/webhooks/github/<rivet-project>
POST /api/v1/webhooks/gitlab/<rivet-project>
```

GitHub accepts signed `push` and `pull_request` deliveries (and acknowledges
`ping`) using `X-Hub-Signature-256`, `X-GitHub-Event`, and `X-GitHub-Delivery`.
GitLab accepts `Push Hook`, `Tag Push Hook`, and `Merge Request Hook` deliveries
using the signed
`webhook-id`/`webhook-timestamp`/`webhook-signature` headers; the legacy
`X-Gitlab-Token` form is also accepted for installations that have not enabled
the newer signing headers. Push adapters validate the commit SHA; PR/MR
adapters accept only opened, reopened, or updated/synchronized actions, fetch
the provider head ref through a bounded refspec, and then normalize to the same
idempotent build admission path. All adapters can attach a default non-secret
Rivet credential ID for the fetch.

Configure the provider keys through private files and, when needed, point each
adapter at its vault credential ID:

```sh
chmod 600 /secure/path/rivet.github-webhook.secret
chmod 600 /secure/path/rivet.gitlab-webhook.secret
cargo run -p rivet -- --data-dir .rivet server \
  --github-webhook-secret-file /secure/path/rivet.github-webhook.secret \
  --gitlab-webhook-secret-file /secure/path/rivet.gitlab-webhook.secret \
  --github-webhook-credential-id github \
  --gitlab-webhook-credential-id gitlab \
  --credentials-file /secure/path/rivet.credentials.vault \
  --credentials-passphrase-file /secure/path/rivet.credentials.passphrase
```

The provider contracts are documented by [GitHub's webhook signature
validation guide](https://docs.github.com/en/webhooks/using-webhooks/validating-webhook-deliveries)
and [GitLab's webhook integration documentation](https://docs.gitlab.com/user/project/integrations/webhooks/).
Upstream-trigger mapping and broader provider event coverage remain future
gates.

SCM credentials use a local passphrase-encrypted vault. The CLI reads the
passphrase and provider secret from private files, so neither value is placed
in shell history or command-line arguments:

```sh
chmod 600 /secure/path/rivet.credentials.passphrase
chmod 600 /secure/path/github.token
cargo run -p rivet -- credential set github \
  --username oauth2 \
  --secret-file /secure/path/github.token \
  --passphrase-file /secure/path/rivet.credentials.passphrase \
  --vault-file /secure/path/rivet.credentials.vault
cargo run -p rivet -- credential list \
  --passphrase-file /secure/path/rivet.credentials.passphrase \
  --vault-file /secure/path/rivet.credentials.vault
cargo run -p rivet -- scm prepare . --fetch --credential-id github \
  --credentials-file /secure/path/rivet.credentials.vault \
  --credentials-passphrase-file /secure/path/rivet.credentials.passphrase
```

Start the server with the same vault and a private passphrase file:

```sh
cargo run -p rivet -- --data-dir .rivet server \
  --credentials-file /secure/path/rivet.credentials.vault \
  --credentials-passphrase-file /secure/path/rivet.credentials.passphrase
```

Build admission and explicit SCM preparation accept only the non-secret
credential ID, for example `{ "remote": "origin", "fetch": true,
"credential_id": "github" }`. Provider PR/MR deliveries additionally carry a
validated `fetch_ref`. Rivet resolves the ID locally, passes HTTP
Basic auth to the Git child process through ephemeral configuration, and
redacts the secret and encoded header from command errors. The vault stores
authenticated ciphertext only. When the server is configured with the vault,
administrators can manage its lifecycle through `GET /api/v1/credentials`,
`PUT /api/v1/credentials/<id>`, and `DELETE /api/v1/credentials/<id>`.
Responses contain only IDs and usernames; replacement and removal require the
administrator permission and append a bounded audit event without recording
the secret. Keychain integration and project-level access control remain
future gates.

The same `--credential-id`, `--credentials-file`, and
`--credentials-passphrase-file` flags can be passed to `rivet run` when a
local build needs an authenticated fetch.

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
API request body. SCM credential IDs are references only; their secret values
are not persisted in build data or API responses.

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
fallback_keys = ["rust-target-default"]
paths = ["target"]

[[artifacts]]
name = "bundle"
paths = ["dist/**"]
```

Parameters are resolved per build and exposed to direct processes as
environment variables. Non-secret values are persisted for history; secret
parameters cannot define defaults, are represented as `[redacted]` in stored
build data and API responses, and are replaced with `***` in emitted logs.
Cache paths use exact project-scoped primary and fallback keys, restore before
the first stage, and save only after a successful build to an atomic archive
under Rivet's local data directory; a missing or corrupt cache never fails the
build. Artifact
files stay inside the pipeline workspace, are copied to local Rivet storage
with a SHA-256 checksum, and are available through the build artifacts API or
`rivet artifacts`. Remote agents package only declared artifact matches and
return them through bounded checksum-verified archive chunks before the build
is marked passed. Retention pruning removes the oldest artifacts from completed
builds under an explicit byte budget; active-build artifacts, symlinks, and
non-regular paths are preserved.

Old local cache archives can be removed with an explicit byte budget; only
regular `.tar` entries are eligible, while symlinks and temporary files are
left untouched:

```sh
cargo run -p rivet -- cache prune --max-bytes 5368709120
cargo run -p rivet -- artifact prune --max-bytes 10737418240
```

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
