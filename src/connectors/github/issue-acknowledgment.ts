import { createPrivateKey, createSign } from "node:crypto";
import { readFile } from "node:fs/promises";
import { z } from "zod";
import type { OutboxEnvelope, OutboxTransport } from "../../outbox/types.js";

export const GITHUB_ISSUE_REACTION_TOPIC = "github.issue-reaction.requested";

const payloadSchema = z.object({
  schemaVersion: z.literal(1),
  tenantId: z.string().min(1),
  eventId: z.string().min(1),
  installationId: z.number().int().positive(),
  repositoryId: z.number().int().positive(),
  repositoryFullName: z.string().regex(/^[A-Za-z0-9](?:[A-Za-z0-9-]{0,38})\/[A-Za-z0-9_.-]+$/),
  issueNumber: z.number().int().positive(),
  subjectType: z.enum(["issue", "pull_request"]).default("issue"),
  content: z.literal("eyes"),
}).strict();

export class GitHubIssueAcknowledgmentTransport implements OutboxTransport {
  constructor(private readonly options: {
    appIdFile: string;
    privateKeyFile: string;
    apiBaseUrl?: string;
    fetch?: typeof globalThis.fetch;
    now?: () => number;
    readFile?: (path: string, encoding: "utf8") => Promise<string>;
  }) {}

  async publish(envelope: Readonly<OutboxEnvelope>, options: { signal: AbortSignal }): Promise<void> {
    if (envelope.topic !== GITHUB_ISSUE_REACTION_TOPIC || envelope.aggregateType !== "github-issue-reaction") {
      throw new Error("Unsupported GitHub issue acknowledgment message");
    }
    const payload = payloadSchema.parse(envelope.payload);
    if (payload.tenantId !== envelope.tenantId || payload.eventId !== envelope.aggregateId) {
      throw new Error("GitHub issue acknowledgment identity mismatch");
    }
    const credentials = await readCredentials(
      this.options.appIdFile,
      this.options.privateKeyFile,
      this.options.readFile ?? readFile,
    );
    const fetch = this.options.fetch ?? globalThis.fetch;
    const apiBaseUrl = this.options.apiBaseUrl ?? "https://api.github.com";
    // GitHub authorizes PR reactions against the pull request resource even though the REST
    // endpoint is under /issues/{issue_number}/reactions.
    const permission = payload.subjectType === "pull_request" ? "pull_requests" : "issues";
    const headers = {
      Accept: "application/vnd.github+json",
      "User-Agent": "dispatch-issue-acknowledgment/1.0",
      "X-GitHub-Api-Version": "2022-11-28",
    };
    const tokenResponse = await fetch(`${apiBaseUrl}/app/installations/${payload.installationId}/access_tokens`, {
      method: "POST",
      headers: { ...headers, Authorization: `Bearer ${appJwt(credentials, this.options.now ?? Date.now)}`, "Content-Type": "application/json" },
      body: JSON.stringify({ repository_ids: [payload.repositoryId], permissions: { [permission]: "write" } }),
      redirect: "error",
      signal: options.signal,
    });
    const tokenData = await responseJson(tokenResponse);
    const expiresAt = Date.parse(tokenData.expires_at);
    if (!tokenResponse.ok || typeof tokenData.token !== "string" || !tokenData.token.startsWith("ghs_")
      || !Number.isFinite(expiresAt) || expiresAt <= (this.options.now ?? Date.now)()) {
      throw new Error(`GitHub installation token request failed with status ${tokenResponse.status}`);
    }
    if (!Array.isArray(tokenData.repositories) || tokenData.repositories.length !== 1 || tokenData.repositories[0]?.id !== payload.repositoryId
      || (tokenData.repository_selection !== undefined && tokenData.repository_selection !== "selected")) {
      throw new Error("GitHub installation token repository scope mismatch");
    }
    if (tokenData.permissions?.[permission] !== "write"
      || Object.entries(tokenData.permissions ?? {}).some(([name, access]) => name !== permission && !(name === "metadata" && access === "read"))) {
      throw new Error("GitHub installation token permissions mismatch");
    }

    const authorization = { ...headers, Authorization: `Bearer ${tokenData.token}` };
    const repositoryResponse = await fetch(`${apiBaseUrl}/repositories/${payload.repositoryId}`, {
      headers: authorization,
      redirect: "error",
      signal: options.signal,
    });
    const repository = await responseJson(repositoryResponse);
    if (!repositoryResponse.ok || repository.id !== payload.repositoryId || repository.full_name !== payload.repositoryFullName) {
      throw new Error("GitHub repository identity mismatch");
    }
    const reactionResponse = await fetch(`${apiBaseUrl}/repos/${payload.repositoryFullName}/issues/${payload.issueNumber}/reactions`, {
      method: "POST",
      headers: { ...authorization, "Content-Type": "application/json" },
      body: JSON.stringify({ content: payload.content }),
      redirect: "error",
      signal: options.signal,
    });
    if (reactionResponse.status !== 200 && reactionResponse.status !== 201) {
      throw new Error(`GitHub issue reaction request failed with status ${reactionResponse.status}`);
    }
  }
}

async function readCredentials(
  appIdFile: string,
  privateKeyFile: string,
  read: (path: string, encoding: "utf8") => Promise<string>,
): Promise<{ appId: number; privateKey: string }> {
  const [appIdValue, privateKey] = await Promise.all([read(appIdFile, "utf8"), read(privateKeyFile, "utf8")]);
  const appId = Number(appIdValue.trim());
  if (!Number.isSafeInteger(appId) || appId < 1) throw new Error("Invalid GitHub App ID credential");
  if (createPrivateKey(privateKey).asymmetricKeyType !== "rsa") throw new Error("Invalid GitHub App private key credential");
  return { appId, privateKey };
}

function appJwt(credentials: { appId: number; privateKey: string }, now: () => number): string {
  const seconds = Math.floor(now() / 1_000);
  const encode = (value: object) => Buffer.from(JSON.stringify(value)).toString("base64url");
  const unsigned = `${encode({ alg: "RS256", typ: "JWT" })}.${encode({ iat: seconds - 60, exp: seconds + 540, iss: credentials.appId })}`;
  const signer = createSign("RSA-SHA256");
  signer.end(unsigned);
  return `${unsigned}.${signer.sign(credentials.privateKey, "base64url")}`;
}

async function responseJson(response: Response): Promise<Record<string, any>> {
  const limit = 2 * 1_024 * 1_024;
  const contentLength = Number(response.headers.get("content-length"));
  if (Number.isFinite(contentLength) && contentLength > limit) throw new Error("GitHub response exceeded size limit");
  const chunks: Uint8Array[] = [];
  let length = 0;
  const reader = response.body?.getReader();
  if (!reader) throw new Error("GitHub returned an empty response");
  while (true) {
    const { done, value } = await reader.read();
    if (done) break;
    length += value.byteLength;
    if (length > limit) {
      await reader.cancel();
      throw new Error("GitHub response exceeded size limit");
    }
    chunks.push(value);
  }
  try {
    const value = JSON.parse(Buffer.concat(chunks, length).toString("utf8"));
    if (value === null || typeof value !== "object" || Array.isArray(value)) throw new Error();
    return value;
  } catch {
    throw new Error("GitHub returned invalid JSON");
  }
}
