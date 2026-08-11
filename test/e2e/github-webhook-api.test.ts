import { createHmac } from "node:crypto";
import { describe, expect, it } from "vitest";
import { planAdmission, type AdmissionCommand, type AdmissionResult } from "../../src/control/admission.js";
import type { PublishedBindingVersion } from "../../src/control/binding.js";
import type { Trigger } from "../../src/control/trigger.js";
import { TriggerNotFoundError } from "../../src/control/trigger.js";
import { mountGitHubWebhookApi, type GitHubWebhookApiStore } from "../../src/connectors/github/api.js";
import { verifyGitHubSignature } from "../../src/connectors/github/signature.js";
import { IdempotencyConflictError } from "../../src/execution/types.js";
import { createOpenApiApp } from "../../src/openapi.js";
import { WorkspaceResolutionError } from "../../src/workspace/resolver.js";

const SECRET_ENV = "DISPATCH_GITHUB_WEBHOOK_SECRET_TEST";
const SECRET = "a sufficiently long test webhook secret";

describe("GitHub webhook API", () => {
  it("admits signed supported events without bearer auth and returns no internals", async () => {
    const store = new FakeStore();
    const app = testApp(store);
    const response = await webhook(app, issuePayload(), { delivery: "delivery-1" });

    expect(response.status).toBe(202);
    expect(await response.text()).toBe("");
    expect(store.lastAdmission).toMatchObject({
      tenantId: "default",
      triggerId: "github",
      event: { id: "delivery-1", type: "com.github.issues.opened" },
      admittedAt: expect.any(String),
      internalEventId: expect.any(String),
      revisionResolution: {
        provider: "github",
        installationId: 10,
        repositoryId: 20,
        repositoryFullName: "acme/widgets",
        cloneUrl: "https://github.com/acme/widgets.git",
        branch: "main",
      },
    });
  });

  it("registers an eyes acknowledgment for opened issues when enabled", async () => {
    const store = new FakeStore();
    const response = await webhook(testApp(store, { [SECRET_ENV]: SECRET }, verifyGitHubSignature, true), issuePayload());
    expect(response.status).toBe(202);
    expect(store.lastAdmission).toMatchObject({
      githubIssueAcknowledgment: {
        subjectType: "issue",
        installationId: 10,
        repositoryId: 20,
        repositoryFullName: "acme/widgets",
        issueNumber: 7,
      },
    });
  });

  it("registers an eyes acknowledgment for opened pull requests when enabled", async () => {
    const store = new FakeStore();
    const payload = closedDeletedForkPayload();
    const response = await webhook(testApp(store, { [SECRET_ENV]: SECRET }, verifyGitHubSignature, true), {
      ...payload,
      action: "opened",
      pull_request: { ...payload.pull_request, state: "open", closed_at: null },
    }, { event: "pull_request" });

    expect(response.status).toBe(202);
    expect(store.lastAdmission).toMatchObject({
      event: { type: "com.github.pull_request.opened" },
      githubIssueAcknowledgment: {
        subjectType: "pull_request",
        installationId: 10,
        repositoryId: 20,
        repositoryFullName: "acme/widgets",
        issueNumber: 7,
      },
    });
  });

  it("admits issue comments as normalized events", async () => {
    const store = new FakeStore();
    const payload = issuePayload();
    const response = await webhook(testApp(store), {
      ...payload,
      action: "created",
      comment: {
        id: 30,
        body: "Continue",
        user: payload.sender,
        created_at: "2026-07-18T10:01:00Z",
        updated_at: "2026-07-18T10:01:00Z",
      },
    }, { event: "issue_comment" });

    expect(response.status).toBe(202);
    expect(store.lastAdmission?.event).toMatchObject({
      type: "com.github.issue_comment.created",
      subject: "issues/7",
      data: { comment: { id: 30, body: "Continue" } },
    });
    expect(store.lastAdmission).toMatchObject({
      revisionResolution: { installationId: 10, repositoryId: 20, branch: "main" },
    });
  });

  it("admits completed workflow runs for exact pull request revisions", async () => {
    const store = new FakeStore();
    const response = await webhook(testApp(store), workflowRunPayload(), { event: "workflow_run" });
    expect(response.status).toBe(202);
    expect(store.lastAdmission?.event).toMatchObject({
      type: "com.github.workflow_run.completed",
      subject: "pulls/7",
      data: { workflowRun: { name: "CI", conclusion: "success", headSha: "a".repeat(40) }, pullRequest: { number: 7 } },
    });
    expect(store.lastAdmission).toMatchObject({
      revisionResolution: { installationId: 10, repositoryId: 20, branch: "main" },
    });
  });

  it("replays the same delivery, conflicts on changed normalized payload, and permits disabled replay", async () => {
    const store = new FakeStore();
    const app = testApp(store);
    expect((await webhook(app, issuePayload(), { delivery: "delivery-2" })).status).toBe(202);
    expect((await webhook(app, issuePayload(), { delivery: "delivery-2" })).status).toBe(202);
    expect(store.admitCalls).toBe(2);

    expect((await webhook(app, issuePayload({ title: "Changed" }), { delivery: "delivery-2" })).status).toBe(409);
    store.trigger = { ...store.trigger!, enabled: false, disabledAt: new Date().toISOString() };
    expect((await webhook(app, issuePayload(), { delivery: "delivery-2" })).status).toBe(202);
    expect((await webhook(app, issuePayload(), { delivery: "delivery-new" })).status).toBe(404);
  });

  it("always verifies the full body and does not distinguish trigger or secret state", async () => {
    const wrongType = new FakeStore();
    wrongType.trigger = { ...wrongType.trigger!, type: "cloudevents.http", config: { schemaVersion: 1 } };
    const invalidSecret = new FakeStore();
    const raw = JSON.stringify(issuePayload());
    const calls: Array<{ body: Uint8Array; secret: string | Uint8Array }> = [];
    const verifier = (_signature: string | null | undefined, secret: string | Uint8Array, body: Uint8Array) => {
      calls.push({ secret, body });
      return true;
    };
    const cases: Array<[GitHubWebhookApiStore, Record<string, string>]> = [
      [new FakeStore(null), { [SECRET_ENV]: SECRET }],
      [wrongType, { [SECRET_ENV]: SECRET }],
      [new FakeStore(), {}],
      [invalidSecret, { [SECRET_ENV]: "too-short" }],
    ];
    const responses = [];
    for (const [store, env] of cases) {
      responses.push(await webhook(testApp(store, env, verifier), issuePayload()));
    }

    for (const response of responses) {
      expect(response.status).toBe(401);
      await expect(response.json()).resolves.toEqual({ error: "Unauthorized" });
    }
    expect(calls).toHaveLength(cases.length);
    for (const call of calls) {
      expect(Buffer.from(call.body).toString("utf8")).toBe(raw);
      expect(Buffer.byteLength(call.secret)).toBeGreaterThanOrEqual(32);
    }
    expect(new Set(calls.map(({ secret }) => Buffer.from(secret).toString("hex"))).size).toBe(1);
  });

  it("does not authorize an unknown trigger with a valid signature for the dummy secret", async () => {
    let dummySecret: string | Uint8Array | undefined;
    const captureVerifier = (_signature: string | null | undefined, secret: string | Uint8Array) => {
      dummySecret = secret;
      return false;
    };
    await webhook(testApp(new FakeStore(null), {}, captureVerifier), issuePayload());

    const raw = JSON.stringify(issuePayload());
    const signature = `sha256=${createHmac("sha256", dummySecret!).update(raw).digest("hex")}`;
    expect(verifyGitHubSignature(signature, dummySecret!, Buffer.from(raw))).toBe(true);
    const response = await webhook(testApp(new FakeStore(null)), issuePayload(), { signature });
    expect(response.status).toBe(401);
    await expect(response.json()).resolves.toEqual({ error: "Unauthorized" });
  });

  it("maps media, encoding, body, and payload failures", async () => {
    expect((await webhook(testApp(), issuePayload(), { contentType: "text/plain" })).status).toBe(415);
    expect((await webhook(testApp(), issuePayload(), { contentEncoding: "gzip" })).status).toBe(415);
    expect((await webhook(testApp(), issuePayload(), { raw: `{"padding":"${"x".repeat(140_000)}"}` })).status).toBe(413);
    expect((await webhook(testApp(), issuePayload(), { raw: "not-json" })).status).toBe(400);
    expect((await webhook(testApp(), issuePayload(), { raw: "[]" })).status).toBe(400);
    expect((await webhook(testApp(), { action: "opened" })).status).toBe(400);
  });

  it("uses fixed 401 responses for all signature authentication failures", async () => {
    const app = testApp();
    for (const signature of [undefined, "sha256=bad", `sha256=${"0".repeat(64)}`]) {
      const response = await webhook(app, issuePayload(), { signature });
      expect(response.status).toBe(401);
      await expect(response.json()).resolves.toEqual({ error: "Unauthorized" });
    }
  });

  it("acknowledges signed ping, unsupported event headers, and unsupported actions only while enabled", async () => {
    const store = new FakeStore();
    const app = testApp(store);
    expect((await webhook(app, { zen: "hello" }, { event: "ping" })).status).toBe(204);
    expect((await webhook(app, { value: true }, { event: "check_suite" })).status).toBe(204);
    expect((await webhook(app, { action: "transferred" })).status).toBe(204);
    expect(store.admitCalls).toBe(0);

    store.trigger = { ...store.trigger!, enabled: false, disabledAt: new Date().toISOString() };
    expect((await webhook(app, { zen: "hello" }, { event: "ping" })).status).toBe(404);
    expect((await webhook(app, { value: true }, { event: "check_suite" })).status).toBe(404);
    expect((await webhook(app, { action: "transferred" })).status).toBe(404);
    expect(store.admitCalls).toBe(0);
  });

  it("enforces OpenAPI path/header schemas before lookup and allows JSON charset plus identity encoding", async () => {
    const store = new FakeStore();
    const app = testApp(store);
    expect((await webhook(app, issuePayload(), { delivery: "" })).status).toBe(400);
    expect((await webhook(app, issuePayload(), { delivery: "has space" })).status).toBe(400);
    expect((await webhook(app, issuePayload(), { delivery: "opaque~delivery" })).status).toBe(400);
    expect((await webhook(app, issuePayload(), { event: "Pull_Request" })).status).toBe(400);
    expect(store.lookupCalls).toBe(0);
    expect((await app.request("/hooks/github/not%20valid", { method: "POST", body: "{}", headers: validHeaders("{}") })).status).toBe(400);
    expect(store.lookupCalls).toBe(0);
    expect((await webhook(app, issuePayload(), { contentType: "application/json; charset=utf-8", contentEncoding: "identity" })).status).toBe(202);
  });

  it("maps workspace and unexpected store failures without leaking details", async () => {
    const workspaceStore = new FakeStore();
    workspaceStore.failure = new WorkspaceResolutionError("private detail");
    const workspace = await webhook(testApp(workspaceStore), issuePayload());
    expect(workspace.status).toBe(422);
    await expect(workspace.json()).resolves.toEqual({ error: "Workspace could not be resolved from event data" });

    const failedStore = new FakeStore();
    failedStore.failure = new Error("database password");
    const response = await webhook(testApp(failedStore), issuePayload());
    expect(response.status).toBe(500);
    await expect(response.json()).resolves.toEqual({ error: "Internal server error" });
  });

  it("returns 422 without falling back to base when a selected deleted-fork head repository is null", async () => {
    const store = new FakeStore();
    store.bindings = [githubHeadWorkspaceBinding()];

    const response = await webhook(testApp(store), closedDeletedForkPayload(), { event: "pull_request" });

    expect(response.status).toBe(422);
    await expect(response.json()).resolves.toEqual({ error: "Workspace could not be resolved from event data" });
    expect(store.admitCalls).toBe(1);
  });
});

class FakeStore implements GitHubWebhookApiStore {
  trigger?: Trigger;
  readonly admissions = new Map<string, { hash: string; result: AdmissionResult }>();
  lastAdmission?: AdmissionCommand;
  admitCalls = 0;
  lookupCalls = 0;
  failure?: Error;
  bindings: PublishedBindingVersion[] = [];

  constructor(trigger: Trigger | null = githubTrigger()) {
    this.trigger = trigger ?? undefined;
  }

  async getTrigger(_tenantId: string, triggerId: string) {
    this.lookupCalls += 1;
    if (!this.trigger || triggerId !== this.trigger.id) return undefined;
    return this.trigger;
  }
  async createTrigger(trigger: Trigger) { this.trigger = trigger; return trigger; }
  async disableTrigger() { return undefined; }
  async admitEvent(command: AdmissionCommand): Promise<AdmissionResult> {
    if (this.failure) throw this.failure;
    this.admitCalls += 1;
    this.lastAdmission = command;
    const previous = this.admissions.get(command.sourceDeduplicationKey);
    if (previous) {
      if (previous.hash !== command.admissionHash) throw new IdempotencyConflictError();
      return { ...previous.result, replayed: true };
    }
    if (!this.trigger?.enabled) throw new TriggerNotFoundError(command.triggerId);
    const result = planAdmission(command, this.bindings);
    this.admissions.set(command.sourceDeduplicationKey, { hash: command.admissionHash, result });
    return result;
  }
}

function validHeaders(raw: string): Record<string, string> {
  return {
    "content-type": "application/json",
    "x-github-delivery": "delivery-default",
    "x-github-event": "issues",
    "x-hub-signature-256": `sha256=${createHmac("sha256", SECRET).update(raw).digest("hex")}`,
  };
}

function testApp(
  store: GitHubWebhookApiStore = new FakeStore(),
  env: Record<string, string> = { [SECRET_ENV]: SECRET },
  verifier: typeof verifyGitHubSignature = verifyGitHubSignature,
  issueAcknowledgmentEnabled = false,
) {
  const app = createOpenApiApp();
  mountGitHubWebhookApi(app, store, (name) => env[name], verifier, issueAcknowledgmentEnabled);
  return app;
}

async function webhook(
  app: ReturnType<typeof createOpenApiApp>,
  payload: unknown,
  options: {
    contentEncoding?: string;
    contentType?: string;
    delivery?: string;
    event?: string;
    raw?: string;
    signature?: string;
  } = {},
) {
  const raw = options.raw ?? JSON.stringify(payload);
  const signature = options.signature === undefined && !("signature" in options)
    ? `sha256=${createHmac("sha256", SECRET).update(raw).digest("hex")}`
    : options.signature;
  const headers: Record<string, string> = {
    "content-type": options.contentType ?? "application/json",
    "x-github-delivery": options.delivery ?? "delivery-default",
    "x-github-event": options.event ?? "issues",
  };
  if (options.contentEncoding !== undefined) headers["content-encoding"] = options.contentEncoding;
  if (signature !== undefined) headers["x-hub-signature-256"] = signature;
  return app.request("/hooks/github/github", { method: "POST", body: raw, headers });
}

function githubTrigger(): Trigger {
  return {
    id: "github",
    tenantId: "default",
    type: "github.app.webhook",
    config: { schemaVersion: 1, webhookSecretEnv: SECRET_ENV },
    enabled: true,
    createdAt: "2026-07-18T10:00:00Z",
    disabledAt: null,
  };
}

function issuePayload(overrides: Record<string, unknown> = {}) {
  const actor = { id: 1, login: "octocat", type: "User" };
  return {
    action: "opened",
    installation: { id: 10 },
    repository: { id: 20, full_name: "acme/widgets", clone_url: "https://github.com/acme/widgets.git", default_branch: "main", private: false },
    sender: actor,
    issue: {
      number: 7,
      title: "Issue",
      body: "Details",
      state: "open",
      user: actor,
      labels: [],
      assignees: [],
      created_at: "2026-07-18T10:00:00Z",
      updated_at: "2026-07-18T10:00:00Z",
      closed_at: null,
      ...overrides,
    },
  };
}

function workflowRunPayload() {
  const actor = { id: 1, login: "octocat", type: "User" };
  const repository = { id: 20, full_name: "acme/widgets", clone_url: "https://github.com/acme/widgets.git", default_branch: "main", private: false };
  return {
    action: "completed",
    installation: { id: 10 },
    repository,
    sender: actor,
    workflow_run: {
      id: 900,
      name: "CI",
      event: "pull_request",
      status: "completed",
      conclusion: "success",
      head_sha: "a".repeat(40),
      head_branch: "feature",
      head_repository: { id: 21, full_name: "contributor/widgets" },
      pull_requests: [{ id: 700, number: 7, head: { ref: "feature", sha: "a".repeat(40) }, base: { ref: "main", sha: "b".repeat(40) } }],
    },
  };
}

function closedDeletedForkPayload() {
  const actor = { id: 1, login: "octocat", type: "User" };
  const branchRepository = { id: 20, full_name: "acme/widgets", clone_url: "https://github.com/acme/widgets.git" };
  return {
    action: "closed",
    installation: { id: 10 },
    repository: { ...branchRepository, default_branch: "main", private: false },
    sender: actor,
    pull_request: {
      id: 700,
      number: 7,
      title: "Closed pull request",
      body: "The source fork was deleted",
      draft: false,
      state: "closed",
      merged: false,
      user: actor,
      head: { sha: "a".repeat(40), ref: "deleted-fork-branch", repo: null },
      base: { sha: "b".repeat(40), ref: "main", repo: branchRepository },
      labels: [],
      assignees: [],
      requested_reviewers: [],
      created_at: "2026-07-17T10:00:00Z",
      updated_at: "2026-07-18T10:00:00Z",
      closed_at: "2026-07-18T10:00:00Z",
      merged_at: null,
    },
  };
}

function githubHeadWorkspaceBinding(): PublishedBindingVersion {
  return {
    id: "github-head-workspace-v1",
    bindingId: "github-head-workspace",
    version: 1,
    tenantId: "default",
    triggerId: "github",
    profile: { id: "reviewer", version: 1 },
    definition: {
      schemaVersion: 1,
      eventTypes: ["com.github.pull_request.closed"],
      filter: { all: [] },
      prompt: { literal: "Review it", includeEvent: "data" },
      workspace: {
        type: "git",
        repository: { url: { path: "/pullRequest/head/repository/cloneUrl" } },
        revision: { commit: { path: "/pullRequest/head/sha" } },
      },
    },
    enabled: true,
    createdAt: "2026-07-18T10:00:00Z",
    disabledAt: null,
  };
}
