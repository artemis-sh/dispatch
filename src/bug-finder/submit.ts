import { readFile } from "node:fs/promises";
import { createPrivateKey, createSign } from "node:crypto";

type Config = {
  dispatchUrl: string;
  eventType: string;
  appIdFile: string;
  defaultBranch: string;
  installationIdFile: string;
  privateKeyFile: string;
  repositoryFullName: string;
  repositoryId: number;
  tokenFile: string;
  triggerId: string;
};

const config = readConfig(process.env);
const [owner, repo] = config.repositoryFullName.split("/");
const [appIdText, installationIdText, privateKey, adminToken] = await Promise.all([
  readFile(config.appIdFile, "utf8"),
  readFile(config.installationIdFile, "utf8"),
  readFile(config.privateKeyFile, "utf8"),
  readFile(config.tokenFile, "utf8"),
]);
const appId = positiveInteger(appIdText.trim(), "GitHub App ID");
const installationId = positiveInteger(installationIdText.trim(), "GitHub installation ID");
if (createPrivateKey(privateKey).asymmetricKeyType !== "rsa") throw new Error("GitHub App private key is invalid");
const jwt = appJwt(appId, privateKey);
const installationResponse = await fetch(`https://api.github.com/app/installations/${installationId}/access_tokens`, {
  method: "POST",
  headers: githubHeaders(jwt),
  signal: AbortSignal.timeout(30_000),
});
if (!installationResponse.ok) throw new Error(`GitHub installation token request failed: ${installationResponse.status}`);
const installation = await installationResponse.json() as { token?: unknown };
if (typeof installation.token !== "string" || installation.token.length === 0) throw new Error("GitHub installation token response was invalid");

const repositoryResponse = await fetch(`https://api.github.com/repos/${encodeURIComponent(owner!)}/${encodeURIComponent(repo!)}`, {
  headers: githubHeaders(installation.token),
  signal: AbortSignal.timeout(30_000),
});
if (!repositoryResponse.ok) throw new Error(`GitHub repository request failed: ${repositoryResponse.status}`);
const repository = await repositoryResponse.json() as { clone_url?: unknown; default_branch?: unknown; full_name?: unknown; id?: unknown };
if (repository.id !== config.repositoryId || repository.full_name !== config.repositoryFullName
  || repository.default_branch !== config.defaultBranch || typeof repository.clone_url !== "string") {
  throw new Error("GitHub repository identity did not match configured bug finder target");
}

const revisionResponse = await fetch(`https://api.github.com/repos/${encodeURIComponent(owner!)}/${encodeURIComponent(repo!)}/commits/${encodeURIComponent(config.defaultBranch)}`, {
  headers: githubHeaders(installation.token),
  signal: AbortSignal.timeout(30_000),
});
if (!revisionResponse.ok) throw new Error(`GitHub revision request failed: ${revisionResponse.status}`);
const revision = await revisionResponse.json() as { sha?: unknown };
if (typeof revision.sha !== "string" || !/^[0-9a-f]{40}$/.test(revision.sha)) throw new Error("GitHub revision response was invalid");

const runId = process.env.BUG_FINDER_RUN_ID?.trim();
if (!runId || !/^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$/.test(runId)) throw new Error("BUG_FINDER_RUN_ID is invalid");
const event = {
  specversion: "1.0",
  id: runId,
  source: "urn:dispatch:bug-finder-cron",
  type: config.eventType,
  datacontenttype: "application/json",
  subject: `repositories/${config.repositoryId}`,
  time: new Date().toISOString(),
  data: {
    schemaVersion: 1,
    repository: {
      id: config.repositoryId,
      fullName: config.repositoryFullName,
      cloneUrl: repository.clone_url,
      defaultBranch: config.defaultBranch,
      revision: revision.sha,
    },
  },
};
const admitted = await fetch(new URL(`/v1/triggers/${encodeURIComponent(config.triggerId)}/events`, config.dispatchUrl), {
  method: "POST",
  headers: {
    authorization: `Bearer ${adminToken.trim()}`,
    "content-type": "application/cloudevents+json",
    "idempotency-key": runId,
  },
  body: JSON.stringify(event),
  signal: AbortSignal.timeout(30_000),
});
  if (!admitted.ok) throw new Error(`Dispatch event admission failed: ${admitted.status}`);
const result = await admitted.json() as { executions?: unknown };
  if (!Array.isArray(result.executions)) throw new Error("Dispatch event admission response was invalid");
console.log(JSON.stringify({ revision: revision.sha, executions: result.executions.length }));

function githubHeaders(token: string): Record<string, string> {
  return { accept: "application/vnd.github+json", authorization: `Bearer ${token}`, "user-agent": "dispatch-bug-finder-cron/1.0", "x-github-api-version": "2022-11-28" };
}

function appJwt(appId: number, privateKey: string): string {
  const seconds = Math.floor(Date.now() / 1_000);
  const encode = (value: object) => Buffer.from(JSON.stringify(value)).toString("base64url");
  const unsigned = `${encode({ alg: "RS256", typ: "JWT" })}.${encode({ iat: seconds - 60, exp: seconds + 540, iss: appId })}`;
  const signer = createSign("RSA-SHA256");
  signer.end(unsigned);
  return `${unsigned}.${signer.sign(privateKey, "base64url")}`;
}

function positiveInteger(value: string, label: string): number {
  if (!/^[1-9][0-9]*$/.test(value)) throw new Error(`${label} is invalid`);
  const parsed = Number(value);
  if (!Number.isSafeInteger(parsed)) throw new Error(`${label} is invalid`);
  return parsed;
}

function required(env: NodeJS.ProcessEnv, name: string): string {
  const value = env[name]?.trim();
  if (!value) throw new Error(`${name} is required`);
  return value;
}

function readConfig(env: NodeJS.ProcessEnv): Config {
  const repositoryFullName = required(env, "BUG_FINDER_REPOSITORY_FULL_NAME");
  if (!/^[^/\s]+\/[^/\s]+$/.test(repositoryFullName)) throw new Error("BUG_FINDER_REPOSITORY_FULL_NAME is invalid");
  const dispatchUrl = new URL(required(env, "BUG_FINDER_DISPATCH_URL"));
  if (!["http:", "https:"].includes(dispatchUrl.protocol) || dispatchUrl.username || dispatchUrl.password || dispatchUrl.search || dispatchUrl.hash) {
    throw new Error("BUG_FINDER_DISPATCH_URL is invalid");
  }
  return {
    dispatchUrl: dispatchUrl.toString(),
    eventType: env.SCHEDULED_AGENT_EVENT_TYPE?.trim() || "dev.dispatch.repository.full_review.requested",
    appIdFile: required(env, "BUG_FINDER_GITHUB_APP_ID_FILE"),
    defaultBranch: required(env, "BUG_FINDER_DEFAULT_BRANCH"),
    installationIdFile: required(env, "BUG_FINDER_GITHUB_INSTALLATION_ID_FILE"),
    privateKeyFile: required(env, "BUG_FINDER_GITHUB_PRIVATE_KEY_FILE"),
    repositoryFullName,
    repositoryId: positiveInteger(required(env, "BUG_FINDER_REPOSITORY_ID"), "BUG_FINDER_REPOSITORY_ID"),
    tokenFile: required(env, "BUG_FINDER_DISPATCH_TOKEN_FILE"),
    triggerId: required(env, "BUG_FINDER_TRIGGER_ID"),
  };
}
