import type { OutboxTransport } from "../../outbox/types.js";

/** Releases pending acknowledgment dependencies without sending a GitHub reaction. */
export const disabledGitHubIssueAcknowledgmentTransport: OutboxTransport = {
  publish: async () => undefined,
};
