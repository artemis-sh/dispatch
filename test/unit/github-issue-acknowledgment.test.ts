import { generateKeyPairSync } from "node:crypto";
import { describe, expect, it, vi } from "vitest";
import { GitHubIssueAcknowledgmentTransport } from "../../src/connectors/github/issue-acknowledgment.js";

const { privateKey } = generateKeyPairSync("rsa", { modulusLength: 2048 });
const pem = privateKey.export({ type: "pkcs8", format: "pem" }).toString();
const envelope = {
  id: "message-1",
  tenantId: "default",
  topic: "github.issue-reaction.requested",
  aggregateType: "github-issue-reaction",
  aggregateId: "event-1",
  payload: {
    schemaVersion: 1,
    tenantId: "default",
    eventId: "event-1",
    installationId: 44,
    repositoryId: 10,
    repositoryFullName: "acme/widgets",
    issueNumber: 7,
    content: "eyes",
  },
  headers: {},
  createdAt: "2026-07-20T00:00:00Z",
} as const;

describe("GitHubIssueAcknowledgmentTransport", () => {
  it.each([200, 201])("mints an exact issues-write token and accepts reaction status %i", async (status) => {
    const fetch = vi.fn()
      .mockResolvedValueOnce(Response.json({
        token: "ghs_token",
        expires_at: "2026-07-20T01:00:00Z",
        repository_selection: "selected",
        repositories: [{ id: 10 }],
        permissions: { issues: "write", metadata: "read" },
      }))
      .mockResolvedValueOnce(Response.json({ id: 10, full_name: "acme/widgets" }))
      .mockResolvedValueOnce(new Response("", { status }));
    const transport = new GitHubIssueAcknowledgmentTransport({
      appIdFile: "app-id",
      privateKeyFile: "private-key",
      fetch,
      now: () => Date.parse("2026-07-20T00:00:00Z"),
      readFile: async (path) => String(path) === "app-id" ? "123" : pem,
    });

    await expect(transport.publish(envelope, { signal: new AbortController().signal })).resolves.toBeUndefined();
    expect(fetch.mock.calls[0]![1]).toMatchObject({
      method: "POST",
      body: JSON.stringify({ repository_ids: [10], permissions: { issues: "write" } }),
    });
    expect(fetch.mock.calls[1]![0]).toBe("https://api.github.com/repositories/10");
    expect(fetch.mock.calls[2]![0]).toBe("https://api.github.com/repos/acme/widgets/issues/7/reactions");
    expect(fetch.mock.calls[2]![1]).toMatchObject({ method: "POST", body: JSON.stringify({ content: "eyes" }) });
  });

  it("mints a pull-requests-write token for a pull request acknowledgment", async () => {
    const fetch = vi.fn()
      .mockResolvedValueOnce(Response.json({
        token: "ghs_token",
        expires_at: "2026-07-20T01:00:00Z",
        repository_selection: "selected",
        repositories: [{ id: 10 }],
        permissions: { pull_requests: "write", metadata: "read" },
      }))
      .mockResolvedValueOnce(Response.json({ id: 10, full_name: "acme/widgets" }))
      .mockResolvedValueOnce(new Response("", { status: 201 }));
    const transport = new GitHubIssueAcknowledgmentTransport({
      appIdFile: "app-id",
      privateKeyFile: "private-key",
      fetch,
      now: () => Date.parse("2026-07-20T00:00:00Z"),
      readFile: async (path) => String(path) === "app-id" ? "123" : pem,
    });

    await expect(transport.publish({
      ...envelope,
      payload: { ...envelope.payload, subjectType: "pull_request" as const },
    }, { signal: new AbortController().signal })).resolves.toBeUndefined();
    expect(fetch.mock.calls[0]![1]).toMatchObject({
      body: JSON.stringify({ repository_ids: [10], permissions: { pull_requests: "write" } }),
    });
    expect(fetch.mock.calls[2]![0]).toBe("https://api.github.com/repos/acme/widgets/issues/7/reactions");
  });

  it("rejects a token with broader permissions before mutating the issue", async () => {
    const fetch = vi.fn().mockResolvedValueOnce(Response.json({
      token: "ghs_token",
      expires_at: "2099-01-01T00:00:00Z",
      repositories: [{ id: 10 }],
      permissions: { issues: "write", contents: "write" },
    }));
    const transport = new GitHubIssueAcknowledgmentTransport({
      appIdFile: "app-id", privateKeyFile: "private-key", fetch,
      readFile: async (path) => String(path) === "app-id" ? "123" : pem,
    });
    await expect(transport.publish(envelope, { signal: new AbortController().signal })).rejects.toThrow(/permissions mismatch/);
    expect(fetch).toHaveBeenCalledTimes(1);
  });

  it("rejects non-acknowledgment outbox messages", async () => {
    const transport = new GitHubIssueAcknowledgmentTransport({ appIdFile: "app-id", privateKeyFile: "private-key" });
    await expect(transport.publish({ ...envelope, topic: "execution.requested" }, { signal: new AbortController().signal }))
      .rejects.toThrow(/Unsupported/);
  });
});
