import { expect, it } from "vitest";
import { normalizeGitHubEvent } from "../../src/connectors/github/normalize.js";

const actor = (id: number, login: string) => ({ id, login, type: "User" });

it("ignores workflow runs whose head repository is unavailable", () => {
  const payload = {
    action: "completed",
    installation: { id: 44 },
    repository: {
      id: 10,
      full_name: "acme/widgets",
      clone_url: "https://github.com/acme/widgets.git",
      default_branch: "main",
      private: false,
    },
    sender: actor(2, "sender"),
    workflow_run: {
      id: 900,
      name: "CI",
      event: "pull_request",
      status: "completed",
      conclusion: "success",
      head_sha: "a".repeat(40),
      head_branch: "feature",
      head_repository: null,
      pull_requests: [],
    },
  };

  expect(normalizeGitHubEvent({
    event: "workflow_run",
    deliveryId: "delivery-1",
    payloadSha256: "a".repeat(64),
    payload,
  })).toBeNull();
});
