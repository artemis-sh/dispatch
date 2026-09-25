# Dispatch

Dispatch is a platform for durable, asynchronous agent execution. Clients
publish immutable OpenCode agent profile and binding versions, then admit
idempotent normalized events. Enabled immutable binding versions are the sole
execution-creation mechanism. PostgreSQL records event and execution state and
transactional outbox work; isolated Kubernetes sandboxes provide the execution
substrate.

The product architecture and roadmap are defined in [`DESIGN.md`](DESIGN.md).

## License

Copyright 2026 Artemis. Dispatch is licensed under the
[Apache License 2.0](LICENSE). See [NOTICE](NOTICE) for attribution. By
submitting a contribution, you agree that it is licensed under the same terms.

## Naming

Dispatch identifiers are the canonical deployment, API, and observability
contracts: `DISPATCH_*` environment variables, Kubernetes labels and
annotations, Helm chart and release names, Prometheus metric names, CloudEvent
types, and database objects. New product features must use neutral domain terms
or explicitly versioned public namespaces rather than embedding the product name
in a new runtime contract.

## Current product surface

The current server provides the binding-driven execution foundation:

- Publish and retrieve immutable agent profile versions.
- Create, retrieve, and disable normalized CloudEvents HTTP triggers.
- Publish, retrieve, and disable immutable binding versions.
- Admit idempotent normalized CloudEvents and retrieve resulting execution state,
  attempt records, and transition history.
- Atomically persist normalized events, executions, lifecycle transitions, and
  transactional outbox messages.
- Request best-effort cancellation of pending and active executions.
- Promote due retries and recover expired execution leases through an embedded
  maintenance loop.
- Expose generated OpenAPI 3.1 documentation.

Event ingress returns `202 Accepted`; it does not wait for or directly create a
Kubernetes workload. Each orchestrator replica runs an embedded fenced
dispatcher by default. It claims queued executions and provisions one
SandboxClaim per attempt. A durable external event bus and destination delivery
remain separate future components.

The current migration history is a pre-production baseline covering profiles,
triggers, binding versions, admitted events, executions, and dispatch state.
Databases created with the pre-execution prototype must be recreated before
upgrading; there is intentionally no compatibility migration because no user
data exists.

## Local development

Install dependencies and typecheck:

```bash
pnpm install
pnpm typecheck
```

Configure PostgreSQL, apply migrations, and run the API:

```bash
export DISPATCH_DATABASE_URL=postgres://dispatch:dispatch@localhost:5432/dispatch
export DISPATCH_ADMIN_TOKEN=development-token
pnpm build
pnpm db:migrate
pnpm dev
```

Run tests with:

```bash
pnpm test:unit
pnpm test:e2e
```

The Kubernetes end-to-end tests use `@testcontainers/k3s`, so Docker or another
Testcontainers-compatible runtime and `kubectl` must be available. If Ryuk
cannot mount the Docker socket, run with `TESTCONTAINERS_RYUK_DISABLED=true`.

## Configuration

| Variable | Default | Purpose |
|---|---:|---|
| `PORT` | `3000` | HTTP port for health, documentation, management, event ingress, and execution reads. |
| `DISPATCH_ADMIN_TOKEN` | unset | Bearer token required by `/v1/*`. There is no runtime-admin API. |
| `DISPATCH_DATABASE_URL` / `DATABASE_URL` | required unless host vars are set | PostgreSQL URL for profile, trigger, binding, event, execution, transition, lease, and outbox state. |
| `DISPATCH_DATABASE_HOST` | unset | Alternative to URL configuration; pair with database user, password, and name. |
| `DISPATCH_DATABASE_PORT` | `5432` | PostgreSQL port with host-based configuration. |
| `DISPATCH_DATABASE_USER` | unset | PostgreSQL user with host-based configuration. |
| `DISPATCH_DATABASE_PASSWORD` | unset | PostgreSQL password with host-based configuration. |
| `DISPATCH_DATABASE_NAME` | unset | PostgreSQL database with host-based configuration. |
| `DISPATCH_DATABASE_SSL` | `false` | Enables PostgreSQL SSL. |
| `DISPATCH_DATABASE_SSL_REJECT_UNAUTHORIZED` | `false` | Verifies the PostgreSQL server certificate when SSL is enabled. Enable in production. |
| `DISPATCH_DATABASE_MIGRATIONS_FOLDER` | `drizzle` | Migration folder used by `pnpm db:migrate`. |
| `DISPATCH_EXECUTION_MAINTENANCE_ENABLED` | `true` | Enables retry promotion and expired-lease recovery. |
| `DISPATCH_EXECUTION_MAINTENANCE_INTERVAL_MS` | `5000` | Delay between maintenance cycles. |
| `DISPATCH_EXECUTION_MAINTENANCE_BATCH_SIZE` | `100` | Maximum rows handled by each maintenance operation per cycle. |
| `DISPATCH_EXECUTION_MAX_ATTEMPTS` | `3` | Maximum execution attempts, including the initial attempt. |
| `DISPATCH_EXECUTION_RETRY_DELAY_MS` | `30000` | Fixed delay before a failed execution is eligible for retry. |
| `DISPATCH_DISPATCHER_ENABLED` | `true` | Enables the embedded fenced dispatcher worker. |
| `DISPATCH_DISPATCHER_IDLE_POLL_MS` | `500` | Delay before polling again when no execution is queued. |
| `DISPATCH_DISPATCHER_LEASE_DURATION_MS` | `60000` | Duration of each execution-attempt lease. |
| `DISPATCH_DISPATCHER_RENEW_INTERVAL_MS` | `20000` | Lease renewal interval; must be shorter than the lease duration. |
| `DISPATCH_DISPATCHER_WORKER_ID` | hostname/PID | Stable lease-owner identity for the process. |
| `DISPATCH_KUBE_NAMESPACE` | `agents` | Namespace reserved for SandboxClaim-based execution. |
| `DISPATCH_SANDBOX_CLAIM_API_VERSION` | `v1alpha1` | Installed agent-sandbox extensions API version. |
| `DISPATCH_OPENCODE_PORT` | `4096` | OpenCode server port used by the execution runtime. |
| `DISPATCH_OPENCODE_DIRECTORY` | `/workspace` | OpenCode working directory in execution sandboxes. |
| `LOG_LEVEL` | `info` | Structured JSON log threshold: `debug`, `info`, `warn`, or `error`. |
| `DISPATCH_RECONCILER_GRACE_MINUTES` | `30` | Grace period used by the standalone SandboxClaim reconciler command. |

Apply pending migrations before serving traffic:

```bash
pnpm build
pnpm db:migrate
```

## HTTP API

Health and generated documentation are public:

```text
GET /healthz
GET /docs
GET /openapi.json
```

`GET /healthz` returns `200 OK` when Dispatch is healthy.

Management routes and normalized event ingress require `Authorization: Bearer
<DISPATCH_ADMIN_TOKEN>`:

```text
POST /v1/agent-profiles/:profileID/versions
GET  /v1/agent-profiles/:profileID/versions/:version
POST /v1/connections
GET  /v1/connections/:connectionID
POST /v1/triggers
GET  /v1/triggers/:triggerID
POST /v1/triggers/:triggerID/disable
POST /v1/bindings/:bindingID/versions
GET  /v1/bindings/:bindingID/versions/:version
POST /v1/bindings/:bindingID/versions/:version/disable
POST /v1/triggers/:triggerID/events
GET  /v1/executions/:id
POST /v1/executions/:id/cancel
```

The public `POST /hooks/github/:triggerID` endpoint accepts GitHub deliveries for
a `github.app.webhook` trigger. It does not accept the bearer token;
GitHub authenticates with `X-Hub-Signature-256` using the trigger's configured
webhook-secret environment variable.

Publish an exact profile version, create a `cloudevents.http` trigger, and
publish an enabled binding version before admitting an event. The binding names
one to 32 exact event types, applies up to 16 conjunctive filters to event
`data`, selects an exact profile version, supplies a literal prompt, and resolves
an empty or Git workspace. Filters use RFC 6901 JSON Pointers with `eq`, `in`, or `exists`;
comparison values are JSON primitives. The prompt's `includeEvent` is `none`,
`data`, or `envelope`. It does not perform template expansion.

Connections are generic, tenant-owned metadata records created with `POST
/v1/connections` and read with `GET /v1/connections/:connectionID`; the generated
OpenAPI document defines their request and response schemas. They contain no raw
credential; for example, a create body may be `{"id":"source-control","type":"custom"}`.
A profile grants connections by mapping each connection ID to a
sidecar that must already be owned by its immutable sandbox template:

```json
"connections": [{ "id": "source-control", "sidecar": "credential-broker" }]
```

At dispatch, Dispatch resolves the records and injects one canonical, non-secret
`DISPATCH_CONNECTIONS` JSON envelope into each selected sidecar. The envelope
contains only that sidecar's sorted connection IDs:

```json
{"refs":["source-control"],"schemaVersion":1,"tenantId":"default"}
```

Resolution is fail closed: a missing connection or invalid mapping prevents
profile publication, and a sidecar name absent from the selected template makes
the controller reject the attempt rather than redirecting access. Profiles name
a concrete `v1beta1` `SandboxWarmPool`. Claim-specific environment injection
forces a cold start from that pool's template, so template-owned sidecars,
mounts, and authorization/runtime configuration are applied together. A pool
with `replicas: 0` makes that cold-start policy explicit without prewarming Pods.

For example, a V1 binding-version request body is:

```json
{
  "version": 1,
  "triggerId": "github",
  "profile": { "id": "coder", "version": 1 },
  "definition": {
    "schemaVersion": 1,
    "eventTypes": ["issue.opened"],
    "filter": {
      "all": [{ "path": "/action", "op": "eq", "value": "opened" }]
    },
    "activeSingleton": {
      "name": "developer-issue",
      "key": ["/repository/id", "/issue/number"]
    },
    "prompt": { "literal": "Handle this issue.", "includeEvent": "data" },
    "workspace": { "type": "empty" }
  }
}
```

Create bindings may declare an `activeSingleton` name and one to 16 JSON
pointers into normalized event `data`. The ordered bounded primitive values form
a tenant-scoped key. A distinct event matching the same name and key is durably
admitted but creates no execution while an existing execution is nonterminal.
Exact replay preserves that original decision, even after the owner terminates;
a later distinct event can create the next execution after terminal release.

A Git workspace selects a repository URL and commit from normalized event
`data`, then persists their resolved values on the execution:

```json
"workspace": {
  "type": "git",
  "repository": { "url": { "path": "/repository/cloneUrl" } },
  "revision": { "commit": { "path": "/headSha" } }
}
```

V1 accepts public HTTPS repositories resolving exclusively to public IPv4 addresses and full 40-character SHA-1 commit object
IDs only. It rejects credentials, mutable refs, local/private hosts, and mixed
public/private DNS results. The sandbox materializer pins a validated DNS answer,
fetches the exact commit without a shell, checks it out detached, and verifies
`HEAD` before OpenCode starts. Git workspace environment injection forces a cold
start from the profile's named pool; private repository credentials, submodules,
Git LFS, and warm-pool Git materialization are not implemented yet.

Bindings may declare a policy-controlled after-turn wait:

```json
"afterTurn": {
  "disposition": "wait",
  "wait": {
    "name": "work-item-lifecycle",
    "correlation": [
      { "name": "repositoryId", "path": "/repository/id" },
      { "name": "workItem", "path": "/issue/number" }
    ],
    "deadlineSeconds": 604800
  }
}
```

After a successful agent turn, the fenced completion transaction loads the
execution's immutable binding, resolves the declared correlation values from the
trusted originating event, marks the attempt `SUCCEEDED`, creates one active
`EventWait`, and moves the execution from `RUNNING` to `WAITING`. The agent
cannot choose or alter this disposition. Waiting has no active attempt lease;
the current default resource behavior releases the SandboxClaim. Cancellation
closes the active wait and immediately cancels the execution. Maintenance
expires overdue waits as `TIMED_OUT`. Execution details expose complete wait
history alongside attempts and transitions.

Wake bindings declare `disposition: "wake"`, an exact wait name, bounded
correlation projections over normalized event data, and either a `continue` or
`complete` action. Admission atomically consumes each matching one-shot wait.
A continuation appends immutable input history and moves `WAITING -> QUEUED`;
it may also resolve a new immutable workspace from the wake event, and retries
use that same input and workspace sequence. A terminal action moves directly to
`COMPLETED`. Exact event replay reloads persisted wake results and never rematches
current bindings or active waits.

Bindings may explicitly opt into busy wake admission. Matching events received
while an execution is queued, provisioning, running, or waiting to retry are
retained as immutable intents. Continuations coalesce to the latest intent,
while terminal completion dominates continuation. Fenced turn completion
atomically applies the pending intent instead of entering `WAITING`.

Wait correlation may contain one write-once connector-supplied value for a
future external identity. Until it is supplied, successful turns enter a
non-matchable pending-context wait. Matching wake events are retained as
immutable offers at their original binding versions and reconciled atomically
when trusted correlation becomes complete.

The GitHub broker registers `create_pull_request` before forwarding the
non-idempotent mutation and refuses to forward an existing registration. Each
attempt has a stable, one-purpose effect capability distinct from its rotating
dispatcher fence, so a checkpointed sandbox retains effect access after
adoption. Ambiguous mutations are not retried.

GitHub issue events do not contain a default-branch commit. A developer binding
can select `/repository/defaultBranchRevision/commit`. When such a binding
matches, Dispatch commits the original normalized event and a durable resolution
request, returns `202`, and creates no execution yet. The revision worker mints
a short-lived installation token restricted to the event repository with
`contents:read`, verifies repository ID, full name, clone URL, and default
branch, resolves the branch to a full commit SHA, then persists the resolution
and creates all matching executions atomically.

Resolution uses fenced leases, retries transient failures, and dead-letters the
request after the configured attempt limit. Exact webhook replays reuse the
original event, pending request, or completed executions and never re-resolve a
completed revision. The original webhook data and admission hash remain
unchanged. Enabled bindings are selected when resolution completes. The SHA is
the default-branch head at resolution time, not a reconstruction of the ref at
webhook emission time.

Callers, connectors, the CLI, and tests all invoke agents through generic
normalized CloudEvent trigger ingress. `POST /v1/triggers/:triggerID/events`
requires `Idempotency-Key` and a structured CloudEvents 1.0 JSON envelope and
returns `202 Accepted`. Its response contains the admitted event, zero or more
executions created by matching enabled binding versions, and `replayed`. Reusing
the key with the same normalized request returns the original result; reusing it
with different content returns `409 Conflict`. There is no
`POST /v1/executions` route.

Disabling a trigger prevents new event admission. A new delivery returns `404
Not Found`, including an event that would be a no-op because it matches no
bindings. An exact replay of an event already persisted durably can still return
its original result with `202 Accepted` and `replayed: true`.

For GitHub App delivery, create a trigger such as:

```json
{
  "id": "github",
  "type": "github.app.webhook",
  "config": {
    "schemaVersion": 1,
    "webhookSecretEnv": "DISPATCH_GITHUB_WEBHOOK_SECRET_PRODUCTION"
  }
}
```

Configure GitHub to send webhooks to
`https://dispatch.example.com/hooks/github/github`. Dispatch verifies the
SHA-256 signature against the raw request bytes before parsing JSON, uses
`X-GitHub-Delivery` to deduplicate retries, and normalizes each supported
delivery to zero or one event. Signed pings and unsupported event/action pairs
are acknowledged with `204 No Content` and produce no event. Issues use
`com.github.issues.<action>` with normalized `repository` and
`issue` data; pull requests use `com.github.pull_request.<action>` with
normalized `repository` and `pullRequest` data. Bindings, rather than trigger
configuration, decide which normalized event types invoke agents.

For a public pull-request checkout, select the contributor repository and exact
head commit from the normalized data:

```json
"workspace": {
  "type": "git",
  "repository": { "url": { "path": "/pullRequest/head/repository/cloneUrl" } },
  "revision": { "commit": { "path": "/pullRequest/head/sha" } }
}
```

The webhook secret authenticates inbound deliveries only. It is not a GitHub
App private key, installation token, or Git credential. V1 Git workspaces remain
limited to public HTTPS repositories and do not use the webhook secret for
cloning or GitHub API access.

Enable issue revision resolution with
`DISPATCH_REVISION_RESOLVER_ENABLED=true` and mount the GitHub App ID and RSA
private key as files referenced by `DISPATCH_GITHUB_APP_ID_FILE` and
`DISPATCH_GITHUB_PRIVATE_KEY_FILE`. The delivery supplies the installation ID.
These credentials remain in the orchestrator and are never added to an
execution, workspace, or OpenCode environment. This resolves immutable
revisions for public workspaces; private repository materialization remains
separate future work.

An agent delegates by using ordinary tools or MCP servers to cause an external
effect that a connector later normalizes into another event. Bindings consume
that event exactly like any other; no special Dispatch delegation MCP is
required. Template-owned sidecars provide standard policy-bounded tool access.
Provider-specific integrations compose those generic sidecars with external tool
servers. The complete GitHub MCP and token-broker stack lives in
[`examples/github-software-factory`](examples/github-software-factory/README.md).
Result destinations are separate: they deliver an existing execution's result
and are not an execution-creation mechanism.

The generated OpenAPI document contains the authoritative request and response
schemas.

`GET /v1/executions/:id` returns the materialized current execution together
with its ordered attempt records and append-only state-transition history. The
execution `state` is the current scheduling/lifecycle state; each attempt has
its own status, so a failed or cancelled attempt can remain in history while a
later attempt determines the execution's current state.

`POST /v1/executions/:id/cancel` requires a JSON object. The object may be empty,
or may contain an optional human-readable reason:

```json
{"reason":"Superseded by a newer pull-request revision"}
```

Cancellation is idempotent for an execution already in `CANCEL_REQUESTED` or
`CANCELLED`. Work in `AWAITING_APPROVAL`, like other work without an active
attempt, moves immediately to `CANCELLED`. Active `PROVISIONING` or `RUNNING`
work first moves to `CANCEL_REQUESTED` while its fenced attempt is interrupted
and cleaned up. The endpoint returns `200 OK` with state `CANCELLED` when
cancellation is already or immediately complete, and `202 Accepted` with state
`CANCEL_REQUESTED` while active cleanup remains. `SUCCEEDED` is intentionally
not cancellable: agent work has already committed, and the delivery lifecycle
is not implemented. It returns `409 Conflict`, as do non-cancellable terminal
outcomes, rather than being rewritten as cancelled.

Active workers learn about cancellation when they renew their execution lease,
so interruption latency is normally bounded by
`DISPATCH_DISPATCHER_RENEW_INTERVAL_MS`, plus in-flight operation latency. The
worker deletes the provisioned workload before acknowledging `CANCELLED` using
its attempt and fencing token; a stale worker cannot acknowledge after losing
its lease. If cleanup fails, the execution remains `CANCEL_REQUESTED` until the
lease expires. Existing claim shutdown behavior and the Kubernetes reconciler
provide workload-cleanup fallbacks; execution maintenance does not itself
delete Kubernetes resources. Consequently, `202 Accepted` confirms that the
cancellation request is durable, not that the workload was deleted immediately.
Recovery adopts an expired `RUNNING` attempt only when both its exact sandbox
workload and OpenCode session were durably checkpointed. The dispatcher rotates
the database fence, transfers the SandboxClaim fence with Kubernetes optimistic
concurrency, and observes the existing session without submitting another
prompt. The session must contain a persisted user/assistant exchange before an
idle session can succeed. Earlier crash windows remain non-adoptable and use
the ordinary failed-attempt/retry path; Dispatch never guesses by replaying a
prompt into a checkpointed session.

Cancellation is best effort at the external-effect boundary. Aborting the
OpenCode request and deleting its sandbox can prevent later work, but cannot
roll back an effect already committed outside Dispatch. For example, a GitHub
mutation accepted before cancellation remains committed and must be reconciled
or compensated separately.

The API currently assigns records to the `default` tenant. Tenant-aware
authentication and authorization remain roadmap work.

## Secrets

Do not embed provider or infrastructure credentials in profile definitions.
Use Kubernetes Secrets, an external secret manager, or workload identity. The
Helm chart can generate and preserve `DISPATCH_ADMIN_TOKEN` for development, but
production deployments should provide a managed Secret and rotate tokens under
their normal credential policy.

Reference a GitHub webhook secret from `orchestrator.extraEnv` with
`valueFrom.secretKeyRef`; do not place the secret value in trigger configuration
or Helm values. Keep webhook authentication, GitHub API/App credentials, and
Git clone credentials separate.

The optional Helm AI gateway authorization path lets sandbox workloads use
short-lived Kubernetes identity instead of receiving model-provider keys.

For connection credentials, operators may put a Secret volume on the
template-owned credential broker only. Do not mount that volume into OpenCode,
the workspace materializer, or Dispatch. Dispatch reads connection metadata, not
Secret values, and its chart RBAC intentionally grants no `secrets` access.
Rotate by updating the operator-managed Secret and starting new cold sandboxes;
revoke by disabling/deleting the external credential before terminating affected
sandboxes. Connection records are create/read-only in V1 and do not provide online
revocation. A sidecar is inside the sandbox trust boundary:
it can observe requests sent to it and misuse every credential it mounts, so use
one narrowly scoped identity per purpose and account for the resulting blast
radius. The GitHub token broker implements short-lived installation tokens
without changing the generic profile-to-sidecar contract.

## Deployment

Build and push the API and OpenCode sandbox images:

```bash
docker build -t ghcr.io/your-org/dispatch:latest .
docker push ghcr.io/your-org/dispatch:latest
docker build -f opencode-sandbox.Dockerfile -t ghcr.io/your-org/opencode-sandbox:latest .
docker push ghcr.io/your-org/opencode-sandbox:latest
```

Provider-specific sidecar stacks should be built and deployed by their example.
The GitHub software factory includes the official GitHub MCP sidecar, its
dependency-free installation-token broker, role-specific tools and permissions,
credential mounts, tests, and image build instructions in
[`examples/github-software-factory`](examples/github-software-factory/README.md).
The generic Helm [`README`](deploy/helm/dispatch/README.md) and
[`sandbox-template.yaml`](deploy/examples/sandbox-template.yaml) remain
provider-neutral.

PostgreSQL is required. The Helm chart can run migrations, deploy an in-cluster
PostgreSQL instance for small installs, or use an external PostgreSQL URL or
Secret. It preserves SandboxClaim RBAC, optional `SandboxTemplate` and
`SandboxWarmPool` management, the SandboxClaim reconciler, AI gateway
authorization, and execution maintenance.

Install the agent-sandbox CRDs and controllers before enabling sandbox
resources:

```bash
TAG=v0.5.2
kubectl apply -f https://github.com/kubernetes-sigs/agent-sandbox/releases/download/$TAG/sandbox.yaml
kubectl apply -f https://github.com/kubernetes-sigs/agent-sandbox/releases/download/$TAG/extensions.yaml
```

Then install the chart:

```bash
helm install dispatch deploy/helm/dispatch \
  --namespace agents --create-namespace \
  --set image.repository=ghcr.io/your-org/dispatch \
  --set image.tag=latest \
  --set secrets.existingSecret=dispatch-secrets
```

See the chart [`README`](deploy/helm/dispatch/README.md) for PostgreSQL,
migrations, RBAC, sandbox resources, reconciler, and AI gateway settings.
