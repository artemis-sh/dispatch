import { createPrivateKey } from "node:crypto";
import { readFile as nodeReadFile } from "node:fs/promises";

const DEFAULT_CREDENTIAL_PATHS = Object.freeze({
  appId: "/var/run/dispatch/github-app/app-id",
  installationId: "/var/run/dispatch/github-app/installation-id",
  privateKey: "/var/run/dispatch/github-app/private-key.pem",
});

function required(env, name) {
  const value = env[name];
  if (typeof value !== "string" || value.length === 0 || value.trim() !== value || /[\u0000-\u001f\u007f]/.test(value)) {
    throw new Error(`Invalid or missing ${name}`);
  }
  return value;
}

function positiveInteger(value, name) {
  if (!/^[1-9][0-9]*$/.test(value)) throw new Error(`Invalid ${name}`);
  const parsed = Number(value);
  if (!Number.isSafeInteger(parsed)) throw new Error(`Invalid ${name}`);
  return parsed;
}

function port(value, name) {
  const parsed = positiveInteger(value, name);
  if (parsed > 65_535) throw new Error(`Invalid ${name}`);
  return parsed;
}

function path(env, name, fallback) {
  return name in env ? required(env, name) : fallback;
}

export function parseStartupConfig(env = process.env) {
  const tenantId = required(env, "DISPATCH_GITHUB_TENANT");
  const connectionRef = required(env, "DISPATCH_GITHUB_CONNECTION");
  const repositoryId = positiveInteger(required(env, "DISPATCH_GITHUB_REPOSITORY_ID"), "DISPATCH_GITHUB_REPOSITORY_ID");
  let grants;
  try {
    grants = JSON.parse(required(env, "DISPATCH_CONNECTIONS"));
  } catch {
    throw new Error("Invalid DISPATCH_CONNECTIONS");
  }
  if (
    grants === null || typeof grants !== "object" || Array.isArray(grants)
    || Object.keys(grants).sort().join(",") !== "refs,schemaVersion,tenantId"
    || grants.schemaVersion !== 1 || grants.tenantId !== tenantId
    || !Array.isArray(grants.refs) || grants.refs.length !== 1 || grants.refs[0] !== connectionRef
  ) throw new Error("DISPATCH_CONNECTIONS does not match this broker");

  const upstream = new URL(env.DISPATCH_GITHUB_MCP_UPSTREAM ?? "http://127.0.0.1:8082/");
  if (upstream.protocol !== "http:" || !["127.0.0.1", "localhost", "[::1]"].includes(upstream.hostname)) {
    throw new Error("DISPATCH_GITHUB_MCP_UPSTREAM must be loopback HTTP");
  }
  const permissionEntries = required(env, "DISPATCH_GITHUB_PERMISSIONS").split(",").map((entry) => {
    const [name, access, extra] = entry.split(":");
    if (extra !== undefined || !/^[a-z_]+$/.test(name ?? "") || !["read", "write"].includes(access ?? "")) {
      throw new Error("Invalid DISPATCH_GITHUB_PERMISSIONS");
    }
    return [name, access];
  });
  const permissions = Object.fromEntries(permissionEntries);
  if (Object.keys(permissions).length !== permissionEntries.length) throw new Error("Invalid DISPATCH_GITHUB_PERMISSIONS");
  if (Object.keys(permissions).length === 0) throw new Error("Invalid DISPATCH_GITHUB_PERMISSIONS");
  const host = env.DISPATCH_GITHUB_BROKER_HOST ?? "127.0.0.1";
  if (!["127.0.0.1", "localhost", "::1"].includes(host)) throw new Error("DISPATCH_GITHUB_BROKER_HOST must be loopback");

  return Object.freeze({
    tenantId,
    connectionRef,
    repositoryId,
    permissions: Object.freeze(permissions),
    upstream: upstream.toString(),
    host,
    port: port(env.DISPATCH_GITHUB_BROKER_PORT ?? "8083", "DISPATCH_GITHUB_BROKER_PORT"),
    maxIssuesCreated: env.DISPATCH_GITHUB_MAX_ISSUES_CREATED === undefined
      ? undefined
      : positiveInteger(required(env, "DISPATCH_GITHUB_MAX_ISSUES_CREATED"), "DISPATCH_GITHUB_MAX_ISSUES_CREATED"),
    mergeCapability: env.DISPATCH_GITHUB_MERGE_CAPABILITY ? mergeCapability(
      env.DISPATCH_GITHUB_MERGE_CAPABILITY,
      repositoryId,
      reviewerIds(required(env, "DISPATCH_GITHUB_MERGE_REVIEWER_IDS")),
    ) : undefined,
    effect: env.DISPATCH_EFFECT_ENDPOINT ? Object.freeze({
      endpoint: effectEndpoint(required(env, "DISPATCH_EFFECT_ENDPOINT")),
      executionId: required(env, "DISPATCH_EXECUTION_ID"),
      token: required(env, "DISPATCH_EFFECT_TOKEN"),
    }) : undefined,
    workspace: env.DISPATCH_WORKSPACE_GIT_COMMIT ? Object.freeze({
      baseCommit: required(env, "DISPATCH_WORKSPACE_GIT_COMMIT"),
      directory: path(env, "DISPATCH_WORKSPACE_DIRECTORY", "/workspace"),
    }) : undefined,
    credentialPaths: Object.freeze({
      appId: path(env, "DISPATCH_GITHUB_APP_ID_FILE", DEFAULT_CREDENTIAL_PATHS.appId),
      installationId: path(env, "DISPATCH_GITHUB_INSTALLATION_ID_FILE", DEFAULT_CREDENTIAL_PATHS.installationId),
      privateKey: path(env, "DISPATCH_GITHUB_PRIVATE_KEY_FILE", DEFAULT_CREDENTIAL_PATHS.privateKey),
    }),
  });
}

function reviewerIds(value) {
  const ids = value.split(",").map((entry) => positiveInteger(entry, "DISPATCH_GITHUB_MERGE_REVIEWER_IDS"));
  if (ids.length === 0 || new Set(ids).size !== ids.length) throw new Error("Invalid DISPATCH_GITHUB_MERGE_REVIEWER_IDS");
  return ids;
}

function mergeCapability(value, repositoryId, reviewerIds) {
  let capability;
  try { capability = JSON.parse(value); } catch { throw new Error("Invalid DISPATCH_GITHUB_MERGE_CAPABILITY"); }
  if (capability === null || typeof capability !== "object" || Array.isArray(capability)
    || Object.keys(capability).sort().join(",") !== "commitSha,pullRequestNumber,repositoryFullName,repositoryId,reviewerId,schemaVersion"
    || capability.schemaVersion !== 1 || capability.repositoryId !== repositoryId || !reviewerIds.includes(capability.reviewerId)
    || !Number.isSafeInteger(capability.pullRequestNumber) || capability.pullRequestNumber < 1
    || !Number.isSafeInteger(capability.reviewerId) || capability.reviewerId < 1
    || typeof capability.repositoryFullName !== "string" || !/^[^/\s]+\/[^/\s]+$/.test(capability.repositoryFullName)
    || typeof capability.commitSha !== "string" || !/^[0-9a-f]{40}$/.test(capability.commitSha)) {
    throw new Error("Invalid DISPATCH_GITHUB_MERGE_CAPABILITY");
  }
  return Object.freeze(capability);
}

function effectEndpoint(value) {
  const url = new URL(value);
  if (!["http:", "https:"].includes(url.protocol) || url.username || url.password || url.search || url.hash) throw new Error("Invalid DISPATCH_EFFECT_ENDPOINT");
  return url.toString();
}

export async function readGitHubAppCredentials(paths, readFile = nodeReadFile) {
  let values;
  try {
    values = await Promise.all([readFile(paths.appId, "utf8"), readFile(paths.installationId, "utf8"), readFile(paths.privateKey, "utf8")]);
  } catch {
    throw new Error("Unable to read GitHub App credentials");
  }
  const appId = positiveInteger(values[0].trim(), "GitHub App ID credential");
  const installationId = positiveInteger(values[1].trim(), "GitHub installation ID credential");
  try {
    if (createPrivateKey(values[2]).asymmetricKeyType !== "rsa") throw new Error();
  } catch {
    throw new Error("Invalid GitHub App private key credential");
  }
  return Object.freeze({ appId, installationId, privateKey: values[2] });
}
