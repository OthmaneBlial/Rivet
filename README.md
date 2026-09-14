# Rivet

Rivet is a Rust-first CI/CD automation platform being built independently
from Jenkins.

## Delivery progress

**94% verified** · `███████████████████░`<br>
Weighted evidence score: **94.41 / 100** · displayed conservatively as the
whole-number floor<br>
Measured against the weighted product scope in [ROADMAP.md](ROADMAP.md),
not against a claim of Jenkins feature parity. The percentage only counts
behavior backed by current tests or an exercised local workflow; incomplete
and unverified work remains at zero until it passes its gate.

Last verified update: **2026-09-14** · native pipeline execution, priority-aware
FIFO queue with pause/resume controls,
SQLite history, CLI workflow, headless API, Tauri desktop/logo, and Git/SCM
clone/inspection, server-side project bootstrap cloning into explicit destinations,
persisted build-source identity, recursive Git submodule preparation,
live queue telemetry, durable
event replay, quiet engine offline recovery, explicit Git preparation at build
admission, parameterized builds, local artifact storage and retention pruning,
protected server
transport with safe request IDs and structured method/route/status tracing,
build retry, pre-execution queue cancellation, build artifact
downloads, a priority-ordered queue snapshot and rendered queue control room,
and a light-default desktop theme with an accessible dark-mode
toggle, persistent UTC cron schedules with repository-poll remote/fetch
configuration, server dispatch, desktop schedule controls, signed generic
webhook delivery with idempotent redelivery,
policy-backed API identities with role/project authorization,
secret-parameter redaction/masking, a passphrase-encrypted SCM credential vault
with typed HTTP/SSH credentials, non-secret credential references, project allow-lists,
deployment-specific OS-keychain service/account isolation, and local user accounts
with Argon2id password verification, opaque sessions, and a packaged desktop launch with an
ephemeral loopback engine origin, project-scoped local CI cache restore and
save, explicit Docker/Podman container command assembly with bounded workspace mounts,
and a bounded Jenkinsfile migration analyzer with line-level support findings
plus safe drafts for deterministic shell steps, exposed through the headless
API and rendered in the desktop control room. The server also exposes
administrator-only subprocess extension lifecycle status and start/stop
controls, reflected in the desktop view. A capability-free WASM runtime now
supports an explicit JSON ABI with no host imports, bounded linear memory,
bounded output, and fuel metering. Seven bounded build, detail, log, artifact,
and annotation host methods are available under explicit extension permissions;
annotation writes are persisted with optional stage scope.
The
server also exposes a
versioned agent handshake/heartbeat registry with online/stale state, capacity-aware
matching, a reconnecting heartbeat CLI client, and a rendered fleet view. Pipeline
steps can declare exact remote requirements, including optional CPU cores and
memory in MiB; the local runner refuses those steps until assignment exists. A
matching agent can now reserve executor, CPU, and memory capacity atomically,
receive a bounded workspace archive, execute the assigned pipeline through the
shared Rust runner, and relay typed events, output, and cancellation. Remote artifact
bundles now return through a bounded, checksum-verified channel. Persisted event
projection also ignores exact redelivery of an already recorded domain event by
its SHA-256 identity. If the assigned
agent disconnects, the server makes at most one replacement-agent attempt and
closes unfinished steps, stages, and builds as failed when recovery is
unavailable. That replacement budget and the selected replacement agent now
survive a server restart, so recovery cannot silently reset its retry limit.
Steps may also use a bounded, cancellation-aware retry policy shared by local
and assigned-agent execution. On startup, persisted incomplete builds are
reconciled idempotently so a crashed server cannot leave history stuck forever.
Non-secret remote attempts additionally retain their plan and redacted
parameters and are redispatched with the same build identity when a compatible
agent returns; attempts that require secret values fail closed.
Bounded session-scoped agent delivery now uses versioned delivery IDs, ACKs,
duplicate suppression, and timed retransmission. Remote execution events also
carry a durable attempt identity and monotone sequence; SQLite atomically records
that identity with the build projection, so replay after a server restart is
applied at most once and conflicting reuse is rejected. Bounded retry recovery
is durable locally. Authenticated deployments persist bounded success/failure audit records
without request bodies or Bearer values and expose them only to administrators.
Server deployments can exchange an authenticated API token for a twelve-hour
opaque session token; only its SHA-256 digest and scoped principal snapshot are
persisted, and the current session can be revoked. User accounts and external
identity providers remain future gates.

The repository includes a local-only release gate. It runs the workspace tests,
builds the optimized CLI, builds the desktop web client, checks the native
Tauri host, produces and launches a non-signed macOS `Rivet.app` bundle while
checking its embedded loopback engine, exercises real SCM clone plus
create-project/run/history/inspect/logs workflows against temporary data,
captures live compatibility snapshots, exercises authenticated local deployment
hardening, copies the CLI into a temporary release directory, and writes a
versioned SHA-256 manifest:

```sh
./scripts/local-release-check.sh
```

The release gate first runs `scripts/local-progress-check.sh`, which verifies
that the README percentage matches the weighted evidence calculation, that
repository hygiene checks pass, and that no GitHub Actions workflow has been
added.

The loopback API allow-list includes the normal Vite origin (`1420`) and its
local fallback (`1421`) plus the Tauri origins. Remote deployments must provide
their own exact `--allow-origin` values and authentication.

The deployment smoke checks public health/readiness, token-protected API
routes, exact security/request-ID headers, rejects an unauthenticated public
bind, verifies graceful SIGTERM shutdown, and checks that the token is absent
from server logs and persisted data. This produces a locally verifiable
artifact, not a signed installer, store submission, or hosted CI result.

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
checks the declared permission on every host request. The server exposes
administrator-only runtime status, start/stop actions, and a permission-checked
`POST /api/v1/extensions/<id>/request` invocation route; the desktop view
reflects that state. The bounded WASM runtime accepts only the
documented JSON ABI, denies all module imports, caps module/memory/output
sizes, and meters execution fuel. Filesystem, network, process, and clock
capabilities are not exposed to WASM modules. Seven host methods are now
versioned: `builds.list` (project UUID), `build.details`, `build.logs`,
`build.annotations`, and `build.artifacts` (build UUID) are bounded reads;
`build.annotate` persists a bounded annotation with an optional stage UUID; and
`build.trigger` queues a real project build with validated parameters and
priority. Each requires its matching declared permission, caps returned records,
and wraps the original input with a host protocol version; unknown extension
methods receive their original input unchanged.

The project is being developed as working vertical slices. The current slice
defines a versioned TOML pipeline model with explicit executable/argument
arrays, validated repository-scoped workspaces, dependency-checked stage DAGs,
stable topological execution order, parallel independent stages, deterministic
parameter-gated stages with explicit `skipped` outcomes, persisted domain-safe
IDs, and typed build events. Arbitrary expression conditions remain future
work; this is not Jenkins parity and does not claim production readiness yet.

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
cargo run -p rivet -- inspect rivet --build 1
cargo run -p rivet -- logs rivet --build 1
cargo run -p rivet -- cancel rivet --build 1 --server http://127.0.0.1:7878
cargo run -p rivet -- poll rivet --server http://127.0.0.1:7878 --token-file /secure/path/rivet.token
```

The build command uses the Rust queue, real child processes, live event
projection, and SQLite history. For Git repositories, the build record also
captures the commit, reference, remote, and dirty state observed at admission.
Use `--priority -100..100` to move urgent builds ahead of older queued work;
equal priorities retain FIFO order and per-project/global capacity limits still
apply. The HTTP build body accepts the same `priority` field. Press Ctrl-C
during a running step to exercise the local cancellation path. The `cancel`
command sends one authenticated request to a running server; pass
`--token-file` for a private Bearer token. Mutations are not retried after a
network interruption, avoiding duplicate operator actions.
The `poll` command sends one authenticated repository-change request to a
server, optionally with `--fetch --remote <name> --credential-id <id>`; the
credential is only an ID and its secret stays in the server-side vault.

The process runner streams stdout and stderr independently while retaining at
most 64 KiB of any single line. An unterminated line that exceeds the bound is
emitted with an explicit truncation marker, so a noisy tool cannot grow one
in-memory buffer without limit.

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
database. Liveness and readiness checks remain public; API and WebSocket routes require
`Authorization: Bearer <token>` when authentication is enabled.

The desktop client attaches a bounded request ID to each API call. It retries
only idempotent reads while the engine is unreachable; state-changing requests
are deliberately not retried automatically, so a lost response cannot create
duplicate builds or mutations.

Browser access uses an exact local/Tauri origin allow-list by default,
including the HTTP origin used by Tauri 2 webviews. Add an exact remote
console origin explicitly when needed; wildcard origins are rejected:

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
`GET /api/v1/audit`; request bodies and Bearer values are never recorded.

For a server deployment, local human accounts can be kept in a separate private
file. The CLI stores only Argon2id password verifiers and enforces a minimum
password length, while account lifecycle output never includes password data:

```sh
cargo run -p rivet -- auth user create admin@example.test \
  --role admin --users-file /secure/path/rivet.users.json \
  --password-file /secure/path/admin.password
cargo run -p rivet -- auth user list \
  --users-file /secure/path/rivet.users.json
cargo run -p rivet -- --data-dir .rivet server \
  --bind 0.0.0.0:7878 --auth-users-file /secure/path/rivet.users.json
```

The users file must be a private regular file. Accounts can be enabled,
disabled, removed, or have their password rotated with the corresponding
`auth user` commands; the last active administrator cannot be disabled or
removed. `POST /api/v1/auth/login` accepts a username and password and returns
one opaque twelve-hour session token. Use it as `Authorization: Bearer
<session-token>` and revoke it with `DELETE /api/v1/auth/sessions/current`.
Rivet stores only the session digest in SQLite, expires sessions at lookup time,
prunes expired/revoked rows, and records login and session lifecycle events in
the administrator audit stream. External identity providers remain a future
gate.

Recorded Jenkins/Rivet behavior snapshots can be compared locally with the
same explicit normalizer used by the live capture adapters:

```sh
cargo run -p rivet -- compat compare ./compat/fixtures/sequential-build.json
```

The command exits non-zero when normalized stage, step, parameter, artifact, or
log semantics differ. The checked-in fixture exercises provider status spelling,
CRLF handling, checksum prefixes, and redaction markers. Capture a real Rivet
build or a Jenkins build through their HTTP APIs, using private token files and
private `0600` snapshot outputs:

```sh
cargo run -p rivet -- compat capture-rivet rivet-e2e \
  --server http://127.0.0.1:7878 --build 1 \
  --output /secure/path/rivet.snapshot.json
cargo run -p rivet -- compat capture-jenkins "folder/service" \
  --server https://jenkins.example.test --build 42 \
  --username ci-bot --token-file /secure/path/jenkins.token \
  --output /secure/path/jenkins.snapshot.json
cargo run -p rivet -- compat assemble \
  --scenario sequential-build \
  --jenkins /secure/path/jenkins.snapshot.json \
  --rivet /secure/path/rivet.snapshot.json \
  --output /secure/path/comparison.json
```

Responses are bounded before JSON parsing, Jenkins secret-looking parameter
names are redacted, and console/log capture is opt-in with `--include-logs`.
The local release gate exercises both adapters against a real local Rivet server
and a protocol-compatible Jenkins HTTP fixture; a permanent corpus captured
from a deployed Jenkins instance remains a future gate.

Extension manifests can be loaded by the headless server from an explicit
local directory:

```sh
cargo run -p rivet -- --data-dir .rivet server \
  --extension-manifest-dir /secure/path/rivet-extensions
```

Only `.json` regular files are considered. Manifests are validated for
protocol version, relative entrypoint, unique ID, and declared permissions;
the catalog does not execute or auto-grant an extension.

The versioned API currently exposes public liveness at `/api/v1/health` and a
public SQLite-backed readiness probe at `/api/v1/ready`, plus projects, queued
builds, build details, persisted logs, cancellation, a live queue snapshot, durable replay,
and a per-build WebSocket event stream under `/api/v1/`. It also exposes
persisted UTC cron schedules with create/list/pause/resume/delete operations,
automatic server dispatch, Git repository inspection, and an explicit prepare
operation for fetch/checkout/clean workflows. The CLI also supports explicit
repository cloning into a new or empty destination. Project creation can also
clone a remote repository when the request supplies `repository_url` and an
explicit empty `clone_destination`; optional `branch`, `depth`, `revision`,
`submodules`, and non-secret `credential_id` fields use the same bounded Git
adapter and deployment SSH host-key policy. The server returns
`202 Accepted` when a build is queued. A build request may opt into Git
fetching, revision checkout, workspace cleaning, and explicit recursive
submodule initialization; the default remains inspection-only. Clients read
durable state from the build resource and
subscribe to live events separately.

For repositories that do not have a provider webhook configured, an
authenticated `POST /api/v1/projects/<project>/repository-changes` endpoint
polls the local Git checkout and compares its observed revision with the
latest build. It can optionally fetch a named remote with an explicit Rivet
credential ID, then queues the exact observed revision once. Concurrent or
repeated polls are durably deduplicated and report `queued`, `unchanged`,
`already_queued`, or `already_checking`; this is a local repository poller,
not a claim of provider-side event delivery.

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

The signed generic webhook also accepts an optional upstream reference:
`{"upstream":{"project":"build","build":4,"status":"passed"}}`.
Only a positive, explicitly passed upstream build admits the downstream build;
failed or cancelled upstream deliveries return `ignored`, and unknown statuses
are rejected. This is a bounded trigger contract, not a claim of Jenkins
upstream-job compatibility.

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
Broader provider event coverage and provider-side upstream-trigger mapping
remain future gates.

SCM credentials use a local passphrase-encrypted vault. The CLI reads the
passphrase and provider secret from private files, so neither value is placed
in shell history or command-line arguments:

```sh
chmod 600 /secure/path/rivet.credentials.passphrase
chmod 600 /secure/path/github.token
cargo run -p rivet -- credential set github \
  --username oauth2 \
  --project release \
  --secret-file /secure/path/github.token \
  --passphrase-file /secure/path/rivet.credentials.passphrase \
  --vault-file /secure/path/rivet.credentials.vault
cargo run -p rivet -- credential set deploy-key \
  --kind ssh-key --username git \
  --secret-file /secure/path/deploy.key \
  --passphrase-file /secure/path/rivet.credentials.passphrase \
  --vault-file /secure/path/rivet.credentials.vault
cargo run -p rivet -- credential list \
  --passphrase-file /secure/path/rivet.credentials.passphrase \
  --vault-file /secure/path/rivet.credentials.vault
cargo run -p rivet -- scm prepare . --fetch --credential-id github \
  --project release \
  --credentials-file /secure/path/rivet.credentials.vault \
  --credentials-passphrase-file /secure/path/rivet.credentials.passphrase
cargo run -p rivet -- scm prepare . --fetch --revision main --clean --submodules
cargo run -p rivet -- scm clone https://github.com/example/project.git ./project \
  --branch main --depth 20 --submodules
cargo run -p rivet -- scm prepare . --fetch --credential-id deploy-key \
  --project release \
  --ssh-known-hosts-file /secure/path/known_hosts \
  --credentials-file /secure/path/rivet.credentials.vault \
  --credentials-passphrase-file /secure/path/rivet.credentials.passphrase
```

Start the server with the same vault and a private passphrase file:

```sh
cargo run -p rivet -- --data-dir .rivet server \
  --credentials-file /secure/path/rivet.credentials.vault \
  --credentials-passphrase-file /secure/path/rivet.credentials.passphrase \
  --ssh-known-hosts-file /secure/path/known_hosts
```

Build admission and explicit SCM preparation accept only the non-secret
credential ID, for example `{ "remote": "origin", "fetch": true,
"credential_id": "github" }`. Provider PR/MR deliveries additionally carry a
validated `fetch_ref`. Rivet resolves the ID locally, passes HTTP Basic auth
through ephemeral Git configuration or writes an SSH private key to a private
temporary file for the lifetime of the Git process, and redacts credential
material from command errors. SSH uses `BatchMode` and `IdentitiesOnly`; its
`StrictHostKeyChecking=yes` policy now fails closed against unknown keys. By
default OpenSSH's normal system/user known-hosts files are used. Operators can
provide `--ssh-known-hosts-file` to `rivet run`, `scm prepare`, or `server`; the
file must be a canonical regular non-symlink file that is not world-writable,
and the server applies that deployment trust root to API, build, and webhook
fetches. Rivet passes the file only to OpenSSH and never returns its contents.
The vault stores authenticated ciphertext only. When the server is configured with the vault,
administrators can manage its lifecycle through `GET /api/v1/credentials`,
`PUT /api/v1/credentials/<id>`, and `DELETE /api/v1/credentials/<id>`.
Responses contain only IDs, credential kinds, usernames, and non-secret project scopes;
replacement and removal require the administrator permission and append a
bounded audit event without recording the secret. A credential with no project
scope is global for backwards compatibility; `--project` (repeatable) or the
API `projects` array restricts it to named projects. Build admission, webhooks,
`rivet run`, and `scm prepare --project` enforce that allow-list. The vault
passphrase can also live in the operating-system credential store instead of a
file:

```sh
cargo run -p rivet -- credential keychain-set rivet-server \
  --passphrase-file /secure/path/rivet.credentials.passphrase
cargo run -p rivet -- --data-dir .rivet server \
  --credentials-file /secure/path/rivet.credentials.vault \
  --credentials-keychain-account rivet-server
```

The keychain command uses a private passphrase file only during setup; Rivet
then reads the passphrase from the OS store at server startup. There is no
automatic plaintext-file fallback when the keychain mode is selected. A local
macOS write/read/delete round-trip is covered by an explicit opt-in test;
deployment-specific keychain prompts and policies still require validation on
each target OS. Deployments can isolate the service namespace explicitly by
passing `--service rivet-production` to `credential keychain-set` and
`credential keychain-remove`, then passing
`--credentials-keychain-service rivet-production` to `server`; omitting it
preserves the compatibility service name `Rivet`. A custom service is rejected
unless a keychain account is also selected.

The desktop control room exposes the same admin-only lifecycle when the server
has a vault configured: it shows credential IDs, usernames, and non-secret
scopes, supports secure replacement/removal and project allow-lists, and clears
the entered secret after each save. The API response still contains no secret
material.

The Pipelines view can pass the same non-secret SCM preparation options to a
manual run or retry: explicit remote fetch, revision checkout, controlled
cleanup, and a vault credential ID selected from the loaded summaries.

It also loads the project's declared pipeline parameters through
`GET /api/v1/projects/<name>/parameters`. Non-secret defaults and required
fields are shown in the run form; secret parameters use password inputs, are
sent only with the explicit queue/retry request, and are cleared from the form
after a successful admission. The server remains the source of truth for
unknown or missing values and never returns secret parameter contents.

The same `--credential-id`, `--credentials-file`, and
`--credentials-passphrase-file` flags can be passed to `rivet run` when a
local build needs an authenticated fetch.

Remote agents use a versioned WebSocket contract at
`GET /api/v1/agents/connect`. Agents register capabilities such as operating
system, architecture, Docker availability, labels, and executor capacity, then
send monotone heartbeats. They may also advertise explicit allocatable
`cpu_cores` and `memory_mb` capacity. `GET /api/v1/agents` reports the current
ephemeral registry; silent agents become `stale` after the heartbeat window.
`POST /api/v1/agents/match` accepts exact capability and resource requirements and
excludes stale or saturated agents. Reservations account for every explicitly
requested executor, CPU, and memory unit; an unknown running build fails closed
for resource-constrained matching. A build with a remote step reserves a matching
online agent, transfers the repository workspace in bounded chunks, executes it
with the shared Rust runner, and persists the agent's typed build events and output.
Cancellation is propagated to the agent, and declared artifacts return through
the same bounded transfer with checksum verification before local storage.
After an agent disconnect, the server makes one bounded replacement attempt and
persists a terminal failed state when no replacement is available. The bounded
replacement budget and selected agent are persisted, so a server restart
resumes the current recovery slot instead of granting another one. Non-secret
remote attempts can also be preserved and redispatched with the same build
identity after a server restart. Remote event attempt IDs and sequences are
persisted atomically with the event projection, making replay after restart
idempotent and rejecting a conflicting sequence payload.

Connect a worker for heartbeat and capability discovery:

```sh
cargo run -p rivet -- agent \
  --server ws://127.0.0.1:7878/api/v1/agents/connect \
  --name linux-builder --os linux --arch x86_64 \
  --label build --executors 2 --cpu-cores 8 --memory-mb 16384
```

The CPU and memory flags are optional. A pipeline that declares either resource
dimension only matches an agent that explicitly advertises that dimension; Rivet
does not infer host memory or pretend that an unknown capacity is available.

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
cargo run -p rivet -- schedule create rivet \
  --name poll-main --expression "*/5 * * * *" --trigger repository-poll \
  --remote origin --fetch --credential-id scm-read
cargo run -p rivet -- schedule list rivet
```

Schedules default to `build`, which admits one pipeline run at each due UTC
occurrence. `--trigger repository-poll` instead inspects the project's local
Git checkout at each occurrence and admits a build only for a revision not yet
recorded for that project. Repository-poll schedules persist the selected
remote, whether a fetch is requested, and only the opaque credential ID used
to resolve a vault entry at dispatch time. The API accepts the same values in
a `poll` object, and the desktop schedule form exposes the same controls.

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
retries = 2
retry_delay_seconds = 3
[stages.steps.container]
runtime = "podman"
image = "rust:1.85"

[environment]
RUST_BACKTRACE = "1"
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

Pipeline `environment` entries are non-secret defaults inherited by every
step; step-level `env` entries override them, and resolved build parameters
override pipeline defaults. Reserved `CI`/`RIVET_*` names are controlled by
the runner. Keep secrets in secret parameters or the encrypted credential
vault, never in the versioned pipeline file. Parameters are resolved per
build and exposed to direct processes as environment variables. Non-secret values are persisted for history; secret
parameters cannot define defaults, are represented as `[redacted]` in stored
build data and API responses, and are replaced with `***` in emitted logs.
Failed or timed-out steps may request up to five additional attempts with a
bounded, cancellation-aware delay. Every attempt emits its own step state and
output while the build keeps one stable identity; this is distinct from
retrying a completed build into a new build number.
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

A step can opt into explicit OCI runtime execution with `[stages.steps.container]`.
Set `runtime = "podman"` to use Podman on a machine without Docker; the default
is the legacy `docker` executable for existing pipeline files. The declaration
supports bounded image pull policy, network selection, workspace-relative bind
volumes, environment forwarding, validated working directories, separated
arguments, `--init`, signal proxying, and automatic container cleanup. Local
runtime-shim tests exercise both executable selections and the direct command
handoff without installing Docker, starting a daemon, or pulling an image. Real
Docker/Podman daemon behavior, image policy enforcement, and end-to-end artifact
and cancellation behavior remain unverified on this machine.

Inspect a Jenkinsfile locally before attempting a migration, or request a
safe draft for simple quoted commands:

```sh
cargo run -p rivet -- analyze jenkinsfile --draft ./Jenkinsfile
```

The analyzer emits versioned JSON with supported, partial, and unsupported
constructs, source line numbers, and Rivet mapping guidance. The optional
draft emits a valid Rivetfile for simple, explicitly quoted `sh`/`bat` steps,
static environment assignments, `string`/`password` parameters, and safe
`archiveArtifacts` patterns. Ambiguous commands, dynamic values, unsupported
parameter types, credentials, plugins, and lifecycle behavior stay in warnings.
It never executes Groovy or plugin code; complex migration semantics still
need manual review. Generated declarative stages retain Jenkins' sequential
order through explicit Rivet dependencies, and the fixture suite keeps
unsupported approval stages visible instead of silently dropping them.

Shell parsing is not implicit. A later pipeline feature may add an explicit
shell step with a documented threat boundary; direct process execution is the
safe default.

Inspect the source state behind a project with direct Git arguments:

```sh
cargo run -p rivet -- scm inspect .
cargo run -p rivet -- scm prepare . --revision main --clean
```

`prepare --clean` is intentionally opt-in because it removes untracked files.
The SCM test suite also exercises a real authenticated local HTTP fetch followed
by detached checkout and untracked-file cleanup. Authentication is injected
only into the Git child process; the credential does not enter `.git/config`,
the returned snapshot, or persisted checkout state. Tracked local edits remain
protected and must be resolved before checkout.
