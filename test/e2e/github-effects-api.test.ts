import { describe, expect, it } from "vitest";
import { mountGitHubEffectsApi, type GitHubEffectStore } from "../../src/connectors/github/effects-api.js";
import { createOpenApiApp } from "../../src/openapi.js";

describe("GitHub effects API", () => {
  it("accepts a PR body at the schema maximum despite JSON overhead", async () => {
    const store = new FakeStore();
    const app = createOpenApiApp();
    mountGitHubEffectsApi(app, store);

    const payload = {
      executionId: "execution-1",
      repositoryId: 1,
      repositoryFullName: "acme/repo",
      request: {
        owner: "acme",
        repo: "repo",
        title: "title",
        head: "branch",
        base: "main",
        body: "x".repeat(65_536),
      },
    };
    const raw = JSON.stringify(payload);

    expect(Buffer.byteLength(raw, "utf8")).toBeGreaterThan(64 * 1024);
    const response = await app.request("/internal/v1/github/pull-request-effects", {
      method: "POST",
      headers: { authorization: "Bearer fence", "content-type": "application/json" },
      body: raw,
    });

    expect(response.status).toBe(200);
    expect(store.registerCalls).toBe(1);
  });
});

class FakeStore implements GitHubEffectStore {
  registerCalls = 0;

  async registerGitHubPullRequestEffect() {
    this.registerCalls += 1;
    return { created: true, id: "effect-1", state: "registered" };
  }

  async reportGitHubPullRequestEffect() {
    return { id: "effect-1", state: "reported" };
  }

  async listGitHubIssueLifecycles() {
    return [];
  }
}
