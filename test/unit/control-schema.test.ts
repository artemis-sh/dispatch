import { describe, expect, it } from "vitest";
import { bindingDefinitionSchema, createBindingDefinitionSchema, publishedBindingVersionSchema } from "../../src/control/binding.js";
import { triggerSchema } from "../../src/control/trigger.js";

const validDefinition = {
  schemaVersion: 1,
  eventTypes: ["com.example.change.created"],
  filter: { all: [{ path: "/state", op: "eq", value: "open" }] },
  prompt: { literal: "Review this change.", includeEvent: "data" },
  workspace: { type: "empty" },
} as const;

const validBinding = {
  id: "binding-version-internal-2",
  bindingId: "change-review",
  version: 2,
  tenantId: "acme",
  triggerId: "webhook",
  profile: { id: "reviewer", version: 4 },
  definition: validDefinition,
  enabled: true,
  createdAt: "2026-07-18T10:00:00Z",
  disabledAt: null,
} as const;

describe("triggerSchema", () => {
  it("accepts the exact non-versioned CloudEvents HTTP trigger", () => {
    const trigger = {
      id: "webhook",
      tenantId: "acme",
      type: "cloudevents.http",
      config: { schemaVersion: 1 },
      enabled: true,
      createdAt: "2026-07-18T10:00:00Z",
      disabledAt: null,
    } as const;
    expect(triggerSchema.parse(trigger)).toEqual(trigger);
    expect(triggerSchema.safeParse({ ...trigger, version: 1 }).success).toBe(false);
    expect(triggerSchema.safeParse({ ...trigger, config: { schemaVersion: 2 } }).success).toBe(false);
  });

  it("discriminates and persists GitHub App webhook trigger configuration", () => {
    const trigger = {
      id: "github-app",
      tenantId: "acme",
      type: "github.app.webhook",
      config: { schemaVersion: 1, webhookSecretEnv: "DISPATCH_GITHUB_WEBHOOK_SECRET_PRODUCTION_1" },
      enabled: true,
      createdAt: "2026-07-18T10:00:00Z",
      disabledAt: null,
    } as const;

    expect(triggerSchema.parse(JSON.parse(JSON.stringify(trigger)))).toEqual(trigger);
    expect(triggerSchema.safeParse({ ...trigger, config: { schemaVersion: 1 } }).success).toBe(false);
    expect(triggerSchema.safeParse({ ...trigger, type: "cloudevents.http" }).success).toBe(false);
    expect(triggerSchema.safeParse({ ...trigger, config: { ...trigger.config, webhookSecretEnv: "GITHUB_SECRET" } }).success).toBe(false);
    expect(triggerSchema.safeParse({ ...trigger, config: { ...trigger.config, extra: true } }).success).toBe(false);
  });
});

describe("binding schemas", () => {
  it("keeps trigger and exact profile on the published record, not in definition", () => {
    expect(publishedBindingVersionSchema.parse(validBinding)).toEqual(validBinding);
    expect(bindingDefinitionSchema.safeParse({ ...validDefinition, trigger: { id: "webhook" } }).success).toBe(false);
    expect(bindingDefinitionSchema.safeParse({ ...validDefinition, profile: { id: "reviewer", version: 4 } }).success).toBe(false);
  });

  it("requires 1..32 exact event types", () => {
    expect(bindingDefinitionSchema.safeParse({ ...validDefinition, eventTypes: [] }).success).toBe(false);
    expect(bindingDefinitionSchema.safeParse({ ...validDefinition, eventTypes: Array(33).fill("event") }).success).toBe(false);
    expect(bindingDefinitionSchema.safeParse({ ...validDefinition, eventTypes: ["com.example.*"] }).success).toBe(true);
  });

  it("supports positive and negative existence checks against data pointers", () => {
    expect(
      bindingDefinitionSchema.safeParse({
        ...validDefinition,
        filter: { all: [{ path: "/active", op: "exists", value: true }, { path: "/missing", op: "exists", value: false }] },
      }).success,
    ).toBe(true);
    expect(bindingDefinitionSchema.safeParse({ ...validDefinition, filter: { all: [{ path: "/active", op: "exists" }] } }).success).toBe(false);
    expect(bindingDefinitionSchema.safeParse({ ...validDefinition, filter: { all: [{ path: "/labels", op: "contains", value: "ready" }] } }).success).toBe(true);
    expect(bindingDefinitionSchema.safeParse({ ...validDefinition, filter: { all: [{ path: "/labels", op: "containsAny", values: ["easy", "hard"] }] } }).success).toBe(true);
    expect(bindingDefinitionSchema.safeParse({ ...validDefinition, filter: { all: [{ path: "/branch", op: "notStartsWith", value: "dependabot/" }] } }).success).toBe(true);
    expect(bindingDefinitionSchema.safeParse({ ...validDefinition, filter: { all: [{ path: "/labels", op: "containsAny", values: [] }] } }).success).toBe(false);
    expect(bindingDefinitionSchema.safeParse({ ...validDefinition, filter: { all: Array(17).fill({ path: "", op: "exists", value: true }) } }).success).toBe(false);
    expect(bindingDefinitionSchema.safeParse({ ...validDefinition, filter: { all: [{ path: "/bad~2path", op: "exists", value: true }] } }).success).toBe(false);
  });

  it("preserves prompt byte bounds and accepts exact Git selectors", () => {
    expect(bindingDefinitionSchema.safeParse({ ...validDefinition, prompt: { literal: "é".repeat(8_192), includeEvent: "none" } }).success).toBe(true);
    expect(bindingDefinitionSchema.safeParse({ ...validDefinition, prompt: { literal: `é${"x".repeat(16_383)}`, includeEvent: "none" } }).success).toBe(false);
    expect(bindingDefinitionSchema.safeParse({
      ...validDefinition,
      workspace: {
        type: "git",
        repository: { url: { path: "/repository/clone_url" } },
        revision: { commit: { path: "/after" } },
      },
    }).success).toBe(true);
    expect(bindingDefinitionSchema.safeParse({ ...validDefinition, workspace: { type: "git" } }).success).toBe(false);
    expect(bindingDefinitionSchema.safeParse({
      ...validDefinition,
      workspace: {
        type: "git",
        repository: { url: { path: "/bad~2pointer" } },
        revision: { commit: { path: "/after" } },
      },
    }).success).toBe(false);
  });

  it("accepts bounded policy-driven after-turn waits", () => {
    const afterTurn = {
      disposition: "wait",
      wait: {
        name: "developer-pr-lifecycle",
        correlation: [{ name: "repositoryId", path: "/repository/id" }, { name: "issue", path: "/issue/number" }],
        deadlineSeconds: 604_800,
        admitWhileBusy: true,
      },
    } as const;
    expect(bindingDefinitionSchema.safeParse({ ...validDefinition, afterTurn }).success).toBe(true);
    expect(bindingDefinitionSchema.safeParse({ ...validDefinition, afterTurn: { ...afterTurn, disposition: "agent-decides" } }).success).toBe(false);
    expect(bindingDefinitionSchema.safeParse({ ...validDefinition, afterTurn: { ...afterTurn, wait: { ...afterTurn.wait, correlation: [] } } }).success).toBe(false);
    expect(bindingDefinitionSchema.safeParse({ ...validDefinition, afterTurn: { ...afterTurn, wait: { ...afterTurn.wait, correlation: [{ name: "x", path: "/x" }, { name: "x", path: "/y" }] } } }).success).toBe(false);
    expect(bindingDefinitionSchema.safeParse({ ...validDefinition, afterTurn: { ...afterTurn, wait: { ...afterTurn.wait, deadlineSeconds: 0 } } }).success).toBe(false);
    expect(bindingDefinitionSchema.safeParse({ ...validDefinition, afterTurn: { ...afterTurn, wait: { ...afterTurn.wait, extra: true } } }).success).toBe(false);
  });

  it("requires a reconcilable lifecycle for GitHub pull request effects", () => {
    const afterTurn = {
      disposition: "wait",
      wait: {
        name: "developer-pr-lifecycle",
        correlation: [{ name: "issue", path: "/issue/number" }],
        deadlineSeconds: 3_600,
      },
    } as const;
    const definition = { ...validDefinition, requireGitHubPullRequestEffect: true, afterTurn };

    expect(() => createBindingDefinitionSchema.parse(definition)).toThrow(/admitWhileBusy|supplied|pull request effect/i);
    expect(createBindingDefinitionSchema.safeParse({
      ...definition,
      afterTurn: {
        ...afterTurn,
        wait: {
          ...afterTurn.wait,
          admitWhileBusy: true,
          correlation: [
            { name: "issue", path: "/issue/number" },
            { name: "pullRequestNumber", source: "supplied", slot: "primaryPullRequestNumber" },
          ],
        },
      },
    }).success).toBe(true);
  });

  it("accepts bounded active execution singleton keys on create bindings only", () => {
    const activeSingleton = { name: "developer-issue", key: ["/repository/id", "/issue/number"] } as const;
    expect(bindingDefinitionSchema.safeParse({ ...validDefinition, activeSingleton }).success).toBe(true);
    expect(bindingDefinitionSchema.safeParse({ ...validDefinition, activeSingleton: { ...activeSingleton, key: [] } }).success).toBe(false);
    expect(bindingDefinitionSchema.safeParse({ ...validDefinition, activeSingleton: { ...activeSingleton, key: ["/bad~2pointer"] } }).success).toBe(false);
  });

  it("accepts checkpoints only on one-shot create bindings", () => {
    const checkpoint = { name: "repository-audit", key: ["/repository/id"], value: { path: "/revision" },
      advanceOn: "succeeded", unchanged: "skip" } as const;
    expect(bindingDefinitionSchema.safeParse({ ...validDefinition, checkpoint }).success).toBe(true);
    expect(bindingDefinitionSchema.safeParse({ ...validDefinition, checkpoint: { ...checkpoint, key: [] } }).success).toBe(false);
    expect(bindingDefinitionSchema.safeParse({ ...validDefinition, checkpoint, afterTurn: {
      disposition: "wait", wait: { name: "lifecycle", correlation: [{ name: "id", path: "/id" }], deadlineSeconds: 60 },
    } }).success).toBe(false);
  });

  it("accepts bounded wake continuation and terminal policies", () => {
    const wake = {
      disposition: "wake",
      schemaVersion: 1,
      eventTypes: ["com.example.review.submitted"],
      filter: { all: [] },
      wake: {
        waitName: "developer-pr-lifecycle",
        correlation: [{ name: "repositoryId", path: "/repository/id" }, { name: "pullRequest", path: "/pullRequest/number" }],
        action: {
          type: "continue",
          prompt: { literal: "Address review feedback.", includeEvent: "data" },
          workspace: {
            type: "git",
            repository: { url: { path: "/pullRequest/head/repository/cloneUrl" } },
            revision: { commit: { path: "/pullRequest/head/sha" } },
          },
        },
      },
    } as const;
    expect(bindingDefinitionSchema.safeParse(wake).success).toBe(true);
    expect(bindingDefinitionSchema.safeParse({ ...wake, wake: { ...wake.wake, delivery: "active-or-coalesced" } }).success).toBe(true);
    expect(bindingDefinitionSchema.safeParse({ ...wake, wake: { ...wake.wake, delivery: "unknown" } }).success).toBe(false);
    expect(bindingDefinitionSchema.safeParse({ ...wake, wake: { ...wake.wake, action: { type: "complete" } } }).success).toBe(true);
    expect(bindingDefinitionSchema.safeParse({ ...wake, wake: { ...wake.wake, action: { type: "cancel", reason: "PULL_REQUEST_CLOSED_UNMERGED" } } }).success).toBe(true);
    expect(bindingDefinitionSchema.safeParse({ ...wake, wake: { ...wake.wake, delivery: "active-or-coalesced", action: { type: "cancel", reason: "PULL_REQUEST_CLOSED_UNMERGED" } } }).success).toBe(false);
    expect(bindingDefinitionSchema.safeParse({ ...wake, wake: { ...wake.wake, action: { type: "cancel", reason: "pr closed" } } }).success).toBe(false);
    expect(bindingDefinitionSchema.safeParse({ ...wake, wake: { ...wake.wake, action: { type: "complete", workspace: { type: "empty" } } } }).success).toBe(false);
    expect(bindingDefinitionSchema.safeParse({ ...wake, workspace: { type: "empty" } }).success).toBe(false);
    expect(bindingDefinitionSchema.safeParse({ ...wake, activeSingleton: { name: "bad", key: ["/repository/id"] } }).success).toBe(false);
    expect(bindingDefinitionSchema.safeParse({ ...wake, wake: { ...wake.wake, correlation: [] } }).success).toBe(false);
    expect(bindingDefinitionSchema.safeParse({ ...wake, wake: { ...wake.wake, correlation: [{ name: "x", path: "/x" }, { name: "x", path: "/y" }] } }).success).toBe(false);
    expect(bindingDefinitionSchema.safeParse({ ...wake, wake: { ...wake.wake, action: { type: "complete", prompt: { literal: "bad", includeEvent: "none" } } } }).success).toBe(false);
  });
});
