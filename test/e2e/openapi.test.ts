import { describe, expect, it } from "vitest";
import type { Config } from "../../src/config.js";
import { Registry } from "prom-client";
import { createOpenApiApp, mountHealthRoute, mountMetricsRoute, mountOpenApiDocs } from "../../src/openapi.js";
import { mountControlApi, type ControlApiStore } from "../../src/control/api.js";
import { mountGitHubWebhookApi } from "../../src/connectors/github/api.js";

describe("OpenAPI docs", () => {
  it("serves the OpenAPI document", async () => {
    const app = createTestApp();
    mountOpenApiDocs(app);

    const response = await app.request("/openapi.json");
    const body = (await response.json()) as Record<string, unknown>;

    expect(response.status).toBe(200);
    expect(body).toMatchObject({ openapi: "3.1.0", info: { title: "Dispatch API" } });
    expect(Object.keys(body.paths as object)).toEqual([
      "/healthz",
      "/v1/agent-profiles/{profileID}/versions",
      "/v1/agent-profiles/{profileID}/versions/{version}",
      "/v1/executions/{id}",
      "/v1/executions/{id}/cancel",
      "/v1/connections",
      "/v1/connections/{connectionID}",
      "/v1/triggers",
      "/v1/triggers/{triggerID}",
      "/v1/triggers/{triggerID}/disable",
      "/v1/bindings/{bindingID}/versions",
      "/v1/bindings/{bindingID}/versions/{version}",
      "/v1/bindings/{bindingID}/versions/{version}/disable",
      "/v1/triggers/{triggerID}/events",
      "/hooks/github/{triggerID}",
    ]);
    expect(body).toMatchObject({
      paths: {
        "/hooks/github/{triggerID}": {
          post: {
            security: [],
            parameters: expect.arrayContaining([
              expect.objectContaining({ in: "path", name: "triggerID", schema: expect.objectContaining({ pattern: "^[A-Za-z0-9](?:[A-Za-z0-9._-]*[A-Za-z0-9])?$" }) }),
              expect.objectContaining({ in: "header", name: "Content-Type", schema: expect.objectContaining({ pattern: "^application\\/json(?:\\s*;\\s*charset\\s*=\\s*(?:utf-8|\"utf-8\"))?$" }) }),
              expect.objectContaining({ in: "header", name: "X-GitHub-Delivery", schema: expect.objectContaining({ pattern: "^[A-Za-z0-9._:-]+$" }) }),
              expect.objectContaining({ in: "header", name: "X-GitHub-Event", schema: expect.objectContaining({ pattern: "^(?=.{1,128}$)[a-z]+(?:_[a-z]+)*$" }) }),
              expect.objectContaining({ in: "header", name: "X-Hub-Signature-256", schema: expect.objectContaining({ pattern: "^sha256=[0-9a-f]{64}$" }) }),
            ]),
            requestBody: { required: true, content: { "application/json": { schema: { type: "object", additionalProperties: true } } } },
            responses: { "202": {}, "204": {}, "401": {}, "404": {} },
          },
        },
        "/v1/triggers/{triggerID}/events": {
          post: {
            responses: {
              "422": {
                description: "A matching binding's workspace could not be resolved. The event and all of its executions are rejected atomically.",
                content: { "application/json": { schema: { $ref: "#/components/schemas/WorkspaceResolutionError" } } },
              },
            },
          },
        },
        "/v1/executions/{id}": {
          get: {
            responses: { "200": { content: { "application/json": { schema: { $ref: "#/components/schemas/ExecutionDetail" } } } } },
          },
        },
        "/v1/executions/{id}/cancel": {
          post: {
            requestBody: {
              required: true,
              content: { "application/json": { schema: { type: "object", additionalProperties: false } } },
            },
            responses: { "200": {}, "202": {}, "404": {}, "409": {} },
          },
        },
      },
      components: {
        schemas: {
          ExecutionAttempt: {
            type: "object",
            additionalProperties: false,
            properties: expect.not.objectContaining({ fencingToken: expect.anything(), leaseOwner: expect.anything() }),
          },
          ExecutionDetail: {
            type: "object",
            properties: { attempts: { type: "array" }, transitions: { type: "array" }, waits: { type: "array" } },
            required: expect.arrayContaining(["attempts", "transitions", "waits"]),
          },
          WorkspaceResolutionError: {
            type: "object",
            properties: { error: { type: "string" } },
            required: ["error"],
          },
        },
      },
    });
  });

  it("serves Swagger UI", async () => {
    const app = createTestApp();
    mountOpenApiDocs(app);

    const response = await app.request("/docs");
    const body = await response.text();

    expect(response.status).toBe(200);
    expect(body).toContain("SwaggerUIBundle");
    expect(body).toContain("/openapi.json");
  });

  it("rejects schedule triggers when the schedule worker is disabled", async () => {
    const app = createOpenApiApp();
    const store = emptyControlStore();
    mountControlApi(app, testConfig(), store);

    const response = await app.request("/v1/triggers", {
      method: "POST",
      headers: { authorization: "Bearer test-token", "content-type": "application/json" },
      body: JSON.stringify({
        id: "every-minute",
        type: "schedule.cron",
        config: {
          schemaVersion: 1,
          expression: "* * * * *",
          timezone: "UTC",
          misfirePolicy: "skip",
          repository: { installationId: 44, id: 10, fullName: "acme/widgets", defaultBranch: "main" },
        },
      }),
    });

    expect(response.status).toBe(422);
    await expect(response.json()).resolves.toEqual({ error: "Schedule triggers require the schedule worker to be enabled" });
  });

  it("serves simple health", async () => {
    const response = await createTestApp().request("/healthz");

    expect(response.status).toBe(200);
    await expect(response.json()).resolves.toEqual({ ok: true, service: "dispatch" });
  });

  it("serves Prometheus metrics without adding the endpoint to OpenAPI", async () => {
    const app = createTestApp();
    const registry = new Registry();
    mountMetricsRoute(app, registry);
    const response = await app.request("/metrics");
    expect(response.status).toBe(200);
    expect(response.headers.get("content-type")).toContain("text/plain");
  });
});

function createTestApp() {
  const app = createOpenApiApp();
  mountHealthRoute(app);
  mountControlApi(app, testConfig(), emptyControlStore());
  mountGitHubWebhookApi(app, emptyControlStore());
  return app;
}

function emptyControlStore(): ControlApiStore {
  return {
    createConnection: async () => { throw new Error("not used"); },
    getConnection: async () => undefined,
    publishProfileVersion: async () => {
      throw new Error("not used");
    },
    getProfileVersion: async () => undefined,
    getExecution: async () => undefined,
    getExecutionDetail: async () => undefined,
    requestExecutionCancellation: async () => undefined,
    createTrigger: async () => { throw new Error("not used"); },
    getTrigger: async () => undefined,
    disableTrigger: async () => undefined,
    publishBindingVersion: async () => { throw new Error("not used"); },
    getBindingVersion: async () => undefined,
    disableBindingVersion: async () => undefined,
    listBindingCandidates: async () => [],
    admitEvent: async () => { throw new Error("not used"); },
  };
}

function testConfig(): Config {
  return {
    adminToken: "test-token",
    dispatcherEnabled: false,
    dispatcherIdlePollMs: 500,
    dispatcherLeaseDurationMs: 60_000,
    dispatcherRenewIntervalMs: 20_000,
    dispatcherWorkerId: "test-worker",
    githubIssueAcknowledgmentEnabled: false,
    githubIssueAcknowledgmentIdlePollMs: 250,
    githubIssueAcknowledgmentLeaseDurationMs: 60_000,
    githubIssueAcknowledgmentRequestTimeoutMs: 30_000,
    githubIssueAcknowledgmentRetryDelayMs: 5_000,
    scheduleWorkerEnabled: false, scheduleWorkerIdlePollMs: 1_000, scheduleWorkerLeaseDurationMs: 60_000,
    scheduleWorkerRetryDelayMs: 30_000, scheduleWorkerMaxAttempts: 5, scheduleWorkerMaterializeBatchSize: 100, scheduleWorkerId: "test-worker",
    revisionResolverEnabled: false,
    revisionResolverIdlePollMs: 500,
    revisionResolverLeaseDurationMs: 60_000,
    revisionResolverMaxAttempts: 5,
    revisionResolverRequestTimeoutMs: 30_000,
    revisionResolverRetryDelayMs: 30_000,
    revisionResolverWorkerId: "test-worker",
    executionMaintenanceBatchSize: 100,
    executionMaintenanceEnabled: true,
    executionMaintenanceIntervalMs: 5_000,
    executionMaxAttempts: 3,
    executionRetryDelayMs: 30_000,
    claimReadyTimeoutMs: 5_000,
    kubeNamespace: "unused",
    opencodeDirectory: "/workspace",
    opencodePort: 4096,
    port: 3000,
    sandboxClaimApiVersion: "v1alpha1",
  };
}
