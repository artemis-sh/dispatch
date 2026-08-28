import { describe, expect, it } from "vitest";
import type { Config } from "../../src/config.js";
import { ConnectionAlreadyExistsError, ConnectionNotFoundError, type Connection, type CreateConnectionCommand } from "../../src/connection/index.js";
import { mountControlApi, type ControlApiStore } from "../../src/control/api.js";
import { planAdmission, type AdmissionCommand, type AdmissionResult } from "../../src/control/admission.js";
import { BindingVersionAlreadyExistsError, type PublishedBindingVersion } from "../../src/control/binding.js";
import { TriggerAlreadyExistsError, TriggerNotFoundError, type Trigger } from "../../src/control/trigger.js";
import type { PublishProfileVersionCommand } from "../../src/execution/store.js";
import type { RevisionAwareAdmissionCommand } from "../../src/revision/types.js";
import {
  ExecutionCancellationConflictError,
  IdempotencyConflictError,
  ProfileVersionAlreadyExistsError,
  type AgentProfileVersion,
  type ExecutionDetail,
  type RequestExecutionCancellationCommand,
  type RequestExecutionCancellationResult,
} from "../../src/execution/types.js";
import { createOpenApiApp } from "../../src/openapi.js";

describe("public API", () => {
  it("requires bearer auth and removes direct execution submission", async () => {
    const app = testApp();
    expect((await app.request("/v1/triggers/t/events")).status).toBe(401);
    expect((await app.request("/v1/triggers/t/events", { headers: { authorization: "Bearer wrong" } })).status).toBe(401);
    expect((await request(app, "POST", "/v1/executions", {})).status).toBe(404);
  });

  it("retains profile publish/get with strict validation", async () => {
    const app = testApp();
    const published = await request(app, "POST", "/v1/agent-profiles/coder/versions", { version: 1, definition: profileDefinition("coder") });
    expect(published.status).toBe(201);
    expect((await request(app, "GET", "/v1/agent-profiles/coder/versions/1")).body).toEqual(published.body);
    expect((await request(app, "POST", "/v1/agent-profiles/coder/versions", { version: 1, definition: profileDefinition("coder") })).status).toBe(409);
    expect((await request(app, "POST", "/v1/agent-profiles/coder/versions", { version: 2, definition: profileDefinition("coder"), extra: true })).status).toBe(400);
  });

  it("creates and reads strict provider-neutral connections", async () => {
    const app = testApp();
    const created = await request(app, "POST", "/v1/connections", { id: "github-main", type: "github.api" });
    expect(created).toMatchObject({ status: 201, body: { id: "github-main", tenantId: "default", type: "github.api", createdAt: expect.any(String) } });
    expect(Object.keys(created.body as object).sort()).toEqual(["createdAt", "id", "tenantId", "type"]);
    expect((await request(app, "GET", "/v1/connections/github-main")).body).toEqual(created.body);
    expect((await request(app, "POST", "/v1/connections", { id: "github-main", type: "github.api" })).status).toBe(409);
    expect((await request(app, "GET", "/v1/connections/missing")).status).toBe(404);
  });

  it.each([
    { id: "github", type: "github.api", secretRef: "secret" },
    { id: "github", type: "github.api", config: {} },
    { id: "GitHub", type: "github.api" },
    { id: "github_main", type: "github.api" },
    { id: "github", type: "GitHub" },
  ])("rejects invalid connection bodies", async (body) => {
    expect((await request(testApp(), "POST", "/v1/connections", body)).status).toBe(400);
  });

  it("maps a profile's missing connection to 404", async () => {
    const definition = { ...profileDefinition("coder"), connections: [{ id: "missing", sidecar: "github-tools" }] };
    const response = await request(testApp(), "POST", "/v1/agent-profiles/coder/versions", { version: 1, definition });

    expect(response).toMatchObject({ status: 404, body: { error: "Connection missing was not found" } });
  });

  it("creates, reads, and disables triggers", async () => {
    const app = testApp();
    const created = await request(app, "POST", "/v1/triggers", { id: "github", type: "cloudevents.http", config: { schemaVersion: 1 } });
    expect(created.status).toBe(201);
    expect(created.body).toMatchObject({ id: "github", tenantId: "default", enabled: true, disabledAt: null });
    expect((await request(app, "GET", "/v1/triggers/github")).body).toEqual(created.body);
    expect((await request(app, "POST", "/v1/triggers/github/disable")).body).toMatchObject({ enabled: false, disabledAt: expect.any(String) });
    expect((await request(app, "POST", "/v1/triggers/github/disable")).body).toMatchObject({ enabled: false });
    expect((await request(app, "GET", "/v1/triggers/missing")).status).toBe(404);
  });

  it("creates validated UTC cron schedule triggers", async () => {
    const app = testApp();
    const body = { id: "hourly-audit", type: "schedule.cron", config: { schemaVersion: 1, expression: "17 * * * *",
      timezone: "UTC", misfirePolicy: "skip", repository: { installationId: 44, id: 10, fullName: "acme/widgets", defaultBranch: "main" } } };
    const created = await request(app, "POST", "/v1/triggers", body);
    expect(created).toMatchObject({ status: 201, body });
    expect((await request(app, "GET", "/v1/triggers/hourly-audit")).body).toEqual(created.body);
    expect((await request(app, "POST", "/v1/triggers", { ...body, id: "bad", config: { ...body.config, expression: "60 * * * *" } })).status).toBe(400);
    expect((await request(app, "POST", "/v1/triggers/hourly-audit/events", cloudEvent("manual", {}), { "Idempotency-Key": "manual" })).status).toBe(404);
  });

  it("validates GitHub webhook secrets without persisting their values", async () => {
    const store = new FakeControlStore();
    const secret = "é".repeat(16);
    const app = testApp(store, () => secret);
    const body = {
      id: "github-app",
      type: "github.app.webhook",
      config: { schemaVersion: 1, webhookSecretEnv: "DISPATCH_GITHUB_WEBHOOK_SECRET_TEST" },
    };

    const created = await request(app, "POST", "/v1/triggers", body);

    expect(created.status).toBe(201);
    expect(created.body).toMatchObject(body);
    expect(JSON.stringify(store.triggers.get("default:github-app"))).not.toContain(secret);
  });

  it.each([undefined, "x".repeat(31), "x".repeat(1025), `${"x".repeat(32)}\0`])("returns fixed 422 when a GitHub webhook secret is unavailable or invalid", async (secret) => {
    const store = new FakeControlStore();
    const app = testApp(store, () => secret);
    const response = await request(app, "POST", "/v1/triggers", {
      id: "github-app",
      type: "github.app.webhook",
      config: { schemaVersion: 1, webhookSecretEnv: "DISPATCH_GITHUB_WEBHOOK_SECRET_TEST" },
    });

    expect(response).toMatchObject({ status: 422, body: { error: "GitHub webhook secret unavailable" } });
    expect(store.triggers.size).toBe(0);
  });

  it("hides GitHub webhook triggers from generic event ingress", async () => {
    const store = new FakeControlStore();
    const app = testApp(store, () => "x".repeat(32));
    expect((await request(app, "POST", "/v1/triggers", {
      id: "github-app",
      type: "github.app.webhook",
      config: { schemaVersion: 1, webhookSecretEnv: "DISPATCH_GITHUB_WEBHOOK_SECRET_TEST" },
    })).status).toBe(201);

    const response = await request(app, "POST", "/v1/triggers/github-app/events", cloudEvent("push", {}), { "Idempotency-Key": "github-delivery" });

    expect(response.status).toBe(404);
    expect(store.lastAdmission).toBeUndefined();
  });

  it("publishes, reads, and disables exact binding versions", async () => {
    const app = testApp();
    await publishDependencies(app);
    const body = bindingBody();
    const published = await request(app, "POST", "/v1/bindings/issues/versions", body);
    expect(published.status).toBe(201);
    expect(published.body).toMatchObject({ bindingId: "issues", version: 1, triggerId: "github", enabled: true, definition: body.definition });
    expect((await request(app, "GET", "/v1/bindings/issues/versions/1")).body).toEqual(published.body);
    expect((await request(app, "POST", "/v1/bindings/issues/versions", body)).status).toBe(409);
    expect((await request(app, "POST", "/v1/bindings/issues/versions/1/disable")).body).toMatchObject({ enabled: false });
    expect((await request(app, "GET", "/v1/bindings/issues/versions/2")).status).toBe(404);
    expect((await request(app, "POST", "/v1/bindings/missing/versions", { ...body, triggerId: "missing" })).status).toBe(404);
  });

  it("admits normalized events, returns zero matches, and replays idempotently", async () => {
    const store = new FakeControlStore();
    const app = testApp(store);
    await publishDependencies(app);
    await request(app, "POST", "/v1/bindings/issues/versions", bindingBody());
    const event = { ...cloudEvent("issue.opened", { action: "opened" }), traceparent: "00-a1b2c3-01" };

    const admitted = await request(app, "POST", "/v1/triggers/github/events", event, { "Idempotency-Key": "delivery-1" });
    expect(admitted.status).toBe(202);
    expect(admitted.body).toMatchObject({ replayed: false, event: { triggerId: "github", eventId: "evt-1" }, executions: [{ binding: { id: "issues", version: 1 } }], wakes: [], pendingWakes: [] });
    expect(store.lastAdmission?.event).toMatchObject({ datacontenttype: "application/json", traceparent: "00-a1b2c3-01" });

    const execution = (admitted.body as AdmissionResult).executions[0]!;
    const fetched = await request(app, "GET", `/v1/executions/${execution.id}`);
    expect(fetched.body).toMatchObject({ id: execution.id, binding: { id: "issues", version: 1 }, attempts: [], transitions: [] });

    const replay = await request(app, "POST", "/v1/triggers/github/events", event, { "Idempotency-Key": "delivery-1" });
    expect(replay.status).toBe(202);
    expect(replay.body).toMatchObject({ replayed: true, executions: [{ id: execution.id }] });

    await request(app, "POST", "/v1/triggers/github/disable");
    const disabledReplay = await request(app, "POST", "/v1/triggers/github/events", event, { "Idempotency-Key": "delivery-1" });
    expect(disabledReplay.status).toBe(202);
    expect(disabledReplay.body).toMatchObject({ replayed: true, executions: [{ id: execution.id }] });
    expect((await request(app, "POST", "/v1/triggers/github/events", cloudEvent("push", {}), { "Idempotency-Key": "new-disabled" })).status).toBe(404);

    const otherApp = testApp();
    await request(otherApp, "POST", "/v1/triggers", { id: "github", type: "cloudevents.http", config: { schemaVersion: 1 } });
    const unmatched = await request(otherApp, "POST", "/v1/triggers/github/events", cloudEvent("push", {}), { "Idempotency-Key": "delivery-2" });
    expect(unmatched.status).toBe(202);
    expect(unmatched.body).toMatchObject({ replayed: false, executions: [], wakes: [], pendingWakes: [] });
  });

  it("passes disabled revision resolver state to CloudEvent admissions", async () => {
    const store = new FakeControlStore();
    const app = testApp(store);
    await publishDependencies(app);

    expect((await request(app, "POST", "/v1/triggers/github/events", cloudEvent("issue.opened", {}), { "Idempotency-Key": "resolver-disabled" })).status).toBe(202);

    expect(store.lastAdmission && "revisionResolverEnabled" in store.lastAdmission
      ? store.lastAdmission.revisionResolverEnabled
      : undefined).toBe(false);
  });

  it("requires structured CloudEvents and Idempotency-Key and maps conflicts", async () => {
    const app = testApp();
    await request(app, "POST", "/v1/triggers", { id: "github", type: "cloudevents.http", config: { schemaVersion: 1 } });
    const event = cloudEvent("issue.opened", {});
    expect((await request(app, "POST", "/v1/triggers/github/events", event)).status).toBe(400);
    expect((await request(app, "POST", "/v1/triggers/github/events", { ...event, specversion: "0.3" }, { "Idempotency-Key": "x" })).status).toBe(400);
    expect((await request(app, "POST", "/v1/triggers/missing/events", event, { "Idempotency-Key": "x" })).status).toBe(404);
    expect((await request(app, "POST", "/v1/triggers/github/events", event, { "Idempotency-Key": "same" })).status).toBe(202);
    expect((await request(app, "POST", "/v1/triggers/github/events", { ...event, data: { changed: true } }, { "Idempotency-Key": "same" })).status).toBe(409);
  });

  it("requests execution cancellation with a fixed actor and default reason", async () => {
    const store = new FakeControlStore();
    const execution = fakeExecution("running", "RUNNING");
    store.executions.set("default:running", execution);

    const response = await request(testApp(store), "POST", "/v1/executions/running/cancel", {});

    expect(response).toEqual({ body: { id: "running", state: "CANCEL_REQUESTED" }, headers: response.headers, status: 202 });
    expect(store.lastCancellation).toMatchObject({
      actor: "control-api",
      executionId: "running",
      reason: "cancellation requested",
      tenantId: "default",
      requestedAt: expect.any(String),
      transitionId: expect.any(String),
    });
  });

  it("returns public attempt and transition detail without lease credentials", async () => {
    const store = new FakeControlStore();
    const execution = fakeExecution("detailed", "RUNNING");
    execution.attempts.push({
      attempt: 1,
      state: "RUNNING",
      startedAt: execution.createdAt,
      finishedAt: null,
      leaseExpiresAt: execution.updatedAt,
      opencodeSessionId: "session-1",
      workloadName: "sandbox-1",
      diagnostic: null,
    });
    execution.transitions.push({
      id: "transition-1",
      attempt: 1,
      sequence: 4,
      fromState: "PROVISIONING",
      toState: "RUNNING",
      actor: "worker",
      reason: "sandbox ready",
      createdAt: execution.updatedAt,
      traceContext: { traceparent: "trace" },
    });
    execution.waits.push({
      activatedAt: execution.updatedAt,
      attempt: 1,
      correlation: { workItem: 42 },
      deadlineAt: new Date(Date.parse(execution.updatedAt) + 60_000).toISOString(),
      endedAt: null,
      id: "wait-1",
      name: "work-item-lifecycle",
      state: "ACTIVE",
    });
    store.executions.set("default:detailed", execution);

    const response = await request(testApp(store), "GET", "/v1/executions/detailed");

    expect(response).toMatchObject({ status: 200, body: { attempts: [{ attempt: 1, state: "RUNNING" }], transitions: [{ id: "transition-1", toState: "RUNNING" }], waits: [{ id: "wait-1", correlation: { workItem: 42 } }] } });
    expect(JSON.stringify(response.body)).not.toMatch(/fencingToken|leaseOwner/);
  });

  it("returns immediate cancellation, not found, and typed conflicts", async () => {
    const store = new FakeControlStore();
    store.executions.set("default:queued", fakeExecution("queued", "QUEUED"));
    store.executions.set("default:completed", fakeExecution("completed", "COMPLETED"));
    const app = testApp(store);

    expect(await request(app, "POST", "/v1/executions/queued/cancel", { reason: "  no longer needed  " })).toMatchObject({
      body: { id: "queued", state: "CANCELLED" },
      status: 200,
    });
    expect(store.lastCancellation?.reason).toBe("no longer needed");
    expect(await request(app, "POST", "/v1/executions/missing/cancel", {})).toMatchObject({ status: 404 });
    expect(await request(app, "POST", "/v1/executions/completed/cancel", {})).toMatchObject({
      body: { error: "Execution completed cannot be cancelled in its current state" },
      status: 409,
    });
  });

  it("requires a strict cancellation JSON body with a bounded non-blank reason", async () => {
    const app = testApp();
    expect((await request(app, "POST", "/v1/executions/id/cancel")).status).toBe(400);
    expect((await request(app, "POST", "/v1/executions/id/cancel", { extra: true })).status).toBe(400);
    expect((await request(app, "POST", "/v1/executions/id/cancel", { reason: "   " })).status).toBe(400);
    expect((await request(app, "POST", "/v1/executions/id/cancel", { reason: "x".repeat(1025) })).status).toBe(400);
  });

  it.each([
    ["missing selector", { action: "opened", revision: "a".repeat(40) }],
    ["wrong selector type", { action: "opened", repository: 42, revision: "a".repeat(40) }],
    ["invalid repository URL", { action: "opened", repository: "http://github.example/org/repo.git", revision: "a".repeat(40) }],
    ["invalid commit", { action: "opened", repository: "https://github.example/org/repo.git", revision: "main" }],
  ])("returns 422 for %s without admitting any part of the event", async (_case, data) => {
    const store = new FakeControlStore();
    const app = testApp(store);
    await publishDependencies(app);
    await request(app, "POST", "/v1/bindings/valid/versions", bindingBody());
    await request(app, "POST", "/v1/bindings/git/versions", gitBindingBody());

    const response = await request(app, "POST", "/v1/triggers/github/events", cloudEvent("issue.opened", data), { "Idempotency-Key": `workspace-${_case}` });

    expect(response).toMatchObject({ body: { error: "Workspace could not be resolved from event data" }, status: 422 });
    expect(store.admissions.size).toBe(0);
    expect(store.executions.size).toBe(0);
  });

  it.each(["tenantid", "dispatch"])("returns 400 for caller-supplied reserved %s extensions", async (name) => {
    const app = testApp();
    await request(app, "POST", "/v1/triggers", { id: "github", type: "cloudevents.http", config: { schemaVersion: 1 } });
    const event = { ...cloudEvent("issue.opened", {}), [name]: "caller-supplied" };
    expect((await request(app, "POST", "/v1/triggers/github/events", event, { "Idempotency-Key": `reserved-${name}` })).status).toBe(400);
  });

  it("returns 400 for a non-JSON CloudEvent data content type", async () => {
    const app = testApp();
    await request(app, "POST", "/v1/triggers", { id: "github", type: "cloudevents.http", config: { schemaVersion: 1 } });
    const event = { ...cloudEvent("issue.opened", {}), datacontenttype: "text/plain" };
    expect((await request(app, "POST", "/v1/triggers/github/events", event, { "Idempotency-Key": "non-json" })).status).toBe(400);
  });

  it("enforces the body limit and does not leak unexpected failures", async () => {
    const app = testApp();
    expect((await request(app, "POST", "/v1/triggers", { id: "github", type: "cloudevents.http", config: { schemaVersion: 1 }, padding: "x".repeat(140_000) })).status).toBe(413);
    const store = new FakeControlStore();
    store.failure = new Error("database password leaked");
    const response = await request(testApp(store), "GET", "/v1/executions/failed");
    expect(response).toMatchObject({ status: 500, body: { error: "Internal server error" } });
  });
});

class FakeControlStore implements ControlApiStore {
  readonly connections = new Map<string, Connection>();
  readonly profiles = new Map<string, AgentProfileVersion>();
  readonly triggers = new Map<string, Trigger>();
  readonly bindings = new Map<string, PublishedBindingVersion>();
  readonly executions = new Map<string, ExecutionDetail>();
  readonly admissions = new Map<string, { hash: string; result: AdmissionResult }>();
  lastAdmission?: AdmissionCommand | RevisionAwareAdmissionCommand;
  lastCancellation?: RequestExecutionCancellationCommand;
  failure?: Error;

  async createConnection(command: CreateConnectionCommand) { this.maybeFail(); const key = `${command.tenantId}:${command.connection.id}`; if (this.connections.has(key)) throw new ConnectionAlreadyExistsError(command.connection.id); this.connections.set(key, command); return command; }
  async getConnection(tenantId: string, connectionId: string) { this.maybeFail(); return this.connections.get(`${tenantId}:${connectionId}`); }

  async publishProfileVersion(command: PublishProfileVersionCommand): Promise<AgentProfileVersion> {
    this.maybeFail();
    for (const grant of command.definition.connections ?? []) {
      if (!this.connections.has(`${command.tenantId}:${grant.id}`)) throw new ConnectionNotFoundError(grant.id);
    }
    const key = `${command.tenantId}:${command.profileId}:${command.version}`;
    if (this.profiles.has(key)) throw new ProfileVersionAlreadyExistsError(command.profileId, command.version);
    const value = { id: command.id, tenantId: command.tenantId, profile: { id: command.profileId, version: command.version }, definition: command.definition, createdAt: command.createdAt };
    this.profiles.set(key, value);
    return value;
  }
  async getProfileVersion(tenantId: string, profileId: string, version: number) { this.maybeFail(); return this.profiles.get(`${tenantId}:${profileId}:${version}`); }
  async getExecution(tenantId: string, executionId: string) { this.maybeFail(); return this.executions.get(`${tenantId}:${executionId}`); }
  async getExecutionDetail(tenantId: string, executionId: string) { this.maybeFail(); return this.executions.get(`${tenantId}:${executionId}`); }
  async requestExecutionCancellation(command: RequestExecutionCancellationCommand): Promise<RequestExecutionCancellationResult | undefined> {
    this.maybeFail(); this.lastCancellation = command;
    const key = `${command.tenantId}:${command.executionId}`;
    const execution = this.executions.get(key);
    if (!execution) return undefined;
    if (["COMPLETED", "CANCELLED", "TIMED_OUT", "FAILED", "DEAD_LETTERED"].includes(execution.state)) {
      throw new ExecutionCancellationConflictError(command.executionId);
    }
    const requested = execution.state === "PROVISIONING" || execution.state === "RUNNING" || execution.state === "CANCEL_REQUESTED";
    const state = requested ? "CANCEL_REQUESTED" : "CANCELLED";
    this.executions.set(key, { ...execution, state, updatedAt: command.requestedAt });
    return requested
      ? { outcome: "REQUESTED", id: execution.id, state: "CANCEL_REQUESTED" }
      : { outcome: "CANCELLED", id: execution.id, state: "CANCELLED" };
  }
  async createTrigger(trigger: Trigger) { this.maybeFail(); const key = `${trigger.tenantId}:${trigger.id}`; if (this.triggers.has(key)) throw new TriggerAlreadyExistsError(trigger.id); this.triggers.set(key, trigger); return trigger; }
  async getTrigger(tenantId: string, triggerId: string) { this.maybeFail(); return this.triggers.get(`${tenantId}:${triggerId}`); }
  async disableTrigger(tenantId: string, triggerId: string, disabledAt: string) { this.maybeFail(); const key = `${tenantId}:${triggerId}`; const value = this.triggers.get(key); if (!value) return undefined; if (!value.enabled) return value; const disabled = { ...value, enabled: false, disabledAt }; this.triggers.set(key, disabled); return disabled; }
  async publishBindingVersion(binding: PublishedBindingVersion) { this.maybeFail(); const key = `${binding.tenantId}:${binding.bindingId}:${binding.version}`; if (this.bindings.has(key)) throw new BindingVersionAlreadyExistsError(binding.bindingId, binding.version); this.bindings.set(key, binding); return binding; }
  async getBindingVersion(tenantId: string, bindingId: string, version: number) { this.maybeFail(); return this.bindings.get(`${tenantId}:${bindingId}:${version}`); }
  async disableBindingVersion(tenantId: string, bindingId: string, version: number, disabledAt: string) { this.maybeFail(); const key = `${tenantId}:${bindingId}:${version}`; const value = this.bindings.get(key); if (!value) return undefined; if (!value.enabled) return value; const disabled = { ...value, enabled: false, disabledAt }; this.bindings.set(key, disabled); return disabled; }
  async listBindingCandidates(tenantId: string, triggerId: string, eventType: string) { this.maybeFail(); return [...this.bindings.values()].filter((value) => value.tenantId === tenantId && value.triggerId === triggerId && value.enabled && value.definition.eventTypes.includes(eventType)); }
  async admitEvent(command: AdmissionCommand | RevisionAwareAdmissionCommand): Promise<AdmissionResult> {
    this.maybeFail(); this.lastAdmission = command;
    const key = `${command.tenantId}:${command.sourceDeduplicationKey}`;
    const previous = this.admissions.get(key);
    if (previous) { if (previous.hash !== command.admissionHash) throw new IdempotencyConflictError(); return { ...previous.result, replayed: true }; }
    if (!this.triggers.get(`${command.tenantId}:${command.triggerId}`)?.enabled) throw new TriggerNotFoundError(command.triggerId);
    const result = planAdmission(command, await this.listBindingCandidates(command.tenantId, command.triggerId, command.event.type));
    for (const execution of result.executions) this.executions.set(`${execution.tenantId}:${execution.id}`, { ...execution, attempts: [], transitions: [], waits: [] });
    this.admissions.set(key, { hash: command.admissionHash, result });
    return result;
  }
  private maybeFail() { if (this.failure) throw this.failure; }
}

async function publishDependencies(app: ReturnType<typeof createOpenApiApp>) {
  expect((await request(app, "POST", "/v1/agent-profiles/coder/versions", { version: 1, definition: profileDefinition("coder") })).status).toBe(201);
  expect((await request(app, "POST", "/v1/triggers", { id: "github", type: "cloudevents.http", config: { schemaVersion: 1 } })).status).toBe(201);
}
function bindingBody() { return { version: 1, triggerId: "github", profile: { id: "coder", version: 1 }, definition: { schemaVersion: 1, eventTypes: ["issue.opened"], filter: { all: [{ path: "/action", op: "eq", value: "opened" }] }, prompt: { literal: "Handle issue", includeEvent: "data" }, workspace: { type: "empty" } } }; }
function gitBindingBody() { return { ...bindingBody(), definition: { ...bindingBody().definition, workspace: { type: "git", repository: { url: { path: "/repository" } }, revision: { commit: { path: "/revision" } } } } }; }
function cloudEvent(type: string, data: object) { return { specversion: "1.0", id: type === "push" ? "evt-2" : "evt-1", source: "https://github.example/hooks", type, data }; }
function fakeExecution(id: string, state: ExecutionDetail["state"]): ExecutionDetail { const now = new Date().toISOString(); return { id, tenantId: "default", state, binding: { id: "binding", version: 1 }, profile: { id: "profile", version: 1 }, input: { text: "test" }, workspace: { type: "empty" }, eventId: "event", createdAt: now, updatedAt: now, result: null, attempts: [], transitions: [], waits: [] }; }
function profileDefinition(agent: string) { return { schemaVersion: 1 as const, runtime: { agent, opencodeConfig: { agent: { [agent]: { prompt: "Test" } } }, type: "opencode" as const }, sandbox: { templateName: "opencode", warmPool: "opencode-pool" }, permissions: { onRequest: "fail" as const }, timeoutSeconds: 3600 }; }
function testApp(store: ControlApiStore = new FakeControlStore(), readEnvironmentVariable?: (name: string) => string | undefined) { const app = createOpenApiApp(); mountControlApi(app, testConfig(), store, readEnvironmentVariable); return app; }
async function request(app: ReturnType<typeof createOpenApiApp>, method: string, path: string, body?: unknown, headers: Record<string, string> = {}) { const response = await app.request(path, { method, body: body === undefined ? undefined : JSON.stringify(body), headers: { authorization: "Bearer test-token", ...(body === undefined ? {} : { "content-type": "application/json" }), ...headers } }); const text = await response.text(); const contentType = response.headers.get("content-type"); return { body: text && contentType?.includes("json") ? JSON.parse(text) as unknown : text || undefined, headers: response.headers, status: response.status }; }
function testConfig(): Config { return { adminToken: "test-token", dispatcherEnabled: false, dispatcherIdlePollMs: 500, dispatcherLeaseDurationMs: 60_000, dispatcherRenewIntervalMs: 20_000, dispatcherWorkerId: "test-worker", executionMaintenanceBatchSize: 100, executionMaintenanceEnabled: true, executionMaintenanceIntervalMs: 5_000, executionMaxAttempts: 3, executionRetryDelayMs: 30_000, githubIssueAcknowledgmentEnabled: false, githubIssueAcknowledgmentIdlePollMs: 250, githubIssueAcknowledgmentLeaseDurationMs: 60_000, githubIssueAcknowledgmentRequestTimeoutMs: 30_000, githubIssueAcknowledgmentRetryDelayMs: 5_000, scheduleWorkerEnabled: false, scheduleWorkerIdlePollMs: 1_000, scheduleWorkerLeaseDurationMs: 60_000, scheduleWorkerRetryDelayMs: 30_000, scheduleWorkerMaxAttempts: 5, scheduleWorkerMaterializeBatchSize: 100, scheduleWorkerId: "test-worker", revisionResolverEnabled: false, revisionResolverIdlePollMs: 500, revisionResolverLeaseDurationMs: 60_000, revisionResolverMaxAttempts: 5, revisionResolverRequestTimeoutMs: 30_000, revisionResolverRetryDelayMs: 30_000, revisionResolverWorkerId: "test-worker", claimReadyTimeoutMs: 5_000, kubeNamespace: "unused", opencodeDirectory: "/workspace", opencodePort: 4096, port: 3000, sandboxClaimApiVersion: "v1alpha1" }; }
