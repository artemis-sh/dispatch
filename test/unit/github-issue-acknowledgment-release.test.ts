import { describe, expect, it } from "vitest";
import { disabledGitHubIssueAcknowledgmentTransport } from "../../src/connectors/github/issue-acknowledgment-release.js";

describe("disabledGitHubIssueAcknowledgmentTransport", () => {
  it("releases an acknowledgment without making a GitHub request", async () => {
    await expect(disabledGitHubIssueAcknowledgmentTransport.publish({
      aggregateId: "event-1", aggregateType: "github-issue-reaction", createdAt: new Date().toISOString(),
      headers: {}, id: "outbox-1", payload: {}, tenantId: "default", topic: "github.issue-reaction.requested",
    }, { signal: new AbortController().signal })).resolves.toBeUndefined();
  });
});
