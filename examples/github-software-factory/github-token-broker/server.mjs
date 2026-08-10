import http from "node:http";
import net from "node:net";
import { constants } from "node:fs";
import { open, realpath } from "node:fs/promises";
import { resolve, relative, isAbsolute } from "node:path";

const MAX_REQUEST_BYTES = 2 * 1024 * 1024;
const MAX_RESPONSE_BYTES = 8 * 1024 * 1024;
const MAX_WORKSPACE_PUSH_BYTES = 2 * 1024 * 1024;
const RESPONSE_HEADERS = ["content-type", "mcp-session-id", "retry-after"];

async function readRequest(request) {
  const chunks = [];
  let length = 0;
  for await (const chunk of request) {
    length += chunk.length;
    if (length > MAX_REQUEST_BYTES) throw new Error("REQUEST_TOO_LARGE");
    chunks.push(chunk);
  }
  return Buffer.concat(chunks, length);
}

function upstreamReady(upstream) {
  const url = new URL(upstream);
  return new Promise((resolve) => {
    const socket = net.createConnection({ host: url.hostname, port: Number(url.port || 80) });
    const finish = (ready) => {
      socket.destroy();
      resolve(ready);
    };
    socket.setTimeout(1_000, () => finish(false));
    socket.once("connect", () => finish(true));
    socket.once("error", () => finish(false));
  });
}

function copyHeaders(request, token) {
  const headers = new Headers();
  for (const name of ["accept", "content-type", "last-event-id", "mcp-protocol-version", "mcp-session-id"]) {
    const value = request.headers[name];
    if (typeof value === "string") headers.set(name, value);
  }
  headers.set("authorization", `Bearer ${token}`);
  return headers;
}

async function forward(request, body, config, provider, signal) {
  if (request.url !== "/") throw new Error("INVALID_UPSTREAM_PATH");
  const token = await provider.getToken();
  const response = await fetch(config.upstream, {
    method: request.method,
    headers: copyHeaders(request, token),
    body: request.method === "GET" || request.method === "HEAD" ? undefined : body,
    redirect: "manual",
    signal,
  });
  if (response.status === 401) provider.invalidate(token);
  return response;
}

async function pipeResponse(upstream, response, signal, unbounded) {
  const declared = Number(upstream.headers.get("content-length"));
  if (!unbounded && Number.isFinite(declared) && declared > MAX_RESPONSE_BYTES) throw new Error("RESPONSE_TOO_LARGE");
  const headers = {};
  for (const name of RESPONSE_HEADERS) {
    const value = upstream.headers.get(name);
    if (value !== null) headers[name] = value;
  }
  if (!response.destroyed) response.writeHead(upstream.status, headers);
  if (!upstream.body) {
    response.end();
    return;
  }
  const reader = upstream.body.getReader();
  let length = 0;
  while (true) {
    const { done, value } = await reader.read();
    if (done) break;
    length += value.byteLength;
    if (!unbounded && length > MAX_RESPONSE_BYTES) {
      await reader.cancel();
      response.destroy(new Error("GitHub MCP response exceeded size limit"));
      return;
    }
    if (response.destroyed) continue;
    if (!response.write(Buffer.from(value))) {
      await new Promise((resolve, reject) => {
        const cleanup = () => signal.removeEventListener("abort", onAbort);
        const onAbort = () => {
          response.removeListener("drain", onDrain);
          response.removeListener("close", onClose);
          reject(signal.reason);
        };
        const onDrain = () => {
          response.removeListener("close", onClose);
          cleanup();
          resolve();
        };
        const onClose = () => {
          response.removeListener("drain", onDrain);
          cleanup();
          resolve();
        };
        response.once("drain", onDrain);
        response.once("close", onClose);
        signal.addEventListener("abort", onAbort, { once: true });
      });
    }
  }
  if (!response.destroyed) response.end();
}

function createPullRequestArguments(body) {
  let message;
  try { message = JSON.parse(body.toString("utf8")); } catch { return undefined; }
  if (Array.isArray(message)) throw new Error("JSON_RPC_BATCH_NOT_SUPPORTED");
  if (message?.method !== "tools/call" || message?.params?.name !== "create_pull_request") return undefined;
  if (message.jsonrpc !== "2.0" || !(typeof message.id === "string" || typeof message.id === "number")) throw new Error("INVALID_CREATE_PULL_REQUEST");
  const args = message.params.arguments;
  if (!args || typeof args !== "object" || Array.isArray(args)) throw new Error("INVALID_CREATE_PULL_REQUEST");
  for (const field of ["owner", "repo", "title", "head", "base"]) if (typeof args[field] !== "string" || args[field].length === 0) throw new Error("INVALID_CREATE_PULL_REQUEST");
  return { args, id: message.id };
}

function mergePullRequestArguments(body) {
  let message;
  try { message = JSON.parse(body.toString("utf8")); } catch { return undefined; }
  if (Array.isArray(message)) throw new Error("JSON_RPC_BATCH_NOT_SUPPORTED");
  if (message?.method !== "tools/call" || message?.params?.name !== "merge_pull_request") return undefined;
  if (message.jsonrpc !== "2.0" || !(typeof message.id === "string" || typeof message.id === "number")) throw new Error("INVALID_MERGE_PULL_REQUEST");
  const args = message.params.arguments;
  if (!args || typeof args !== "object" || Array.isArray(args)
    || typeof args.owner !== "string" || typeof args.repo !== "string"
    || !Number.isSafeInteger(args.pullNumber) || args.pullNumber < 1
    || (args.merge_method !== undefined && !["merge", "squash", "rebase"].includes(args.merge_method))) throw new Error("INVALID_MERGE_PULL_REQUEST");
  return { args, id: message.id };
}

function createsIssue(body) {
  let message;
  try { message = JSON.parse(body.toString("utf8")); } catch { return false; }
  if (Array.isArray(message)) throw new Error("JSON_RPC_BATCH_NOT_SUPPORTED");
  if (message?.method !== "tools/call" || message?.params?.name !== "issue_write") return false;
  return message.params.arguments?.method === "create";
}

function issueMutationSubject(body) {
  const call = rpcCall(body, "issue_write");
  const args = call?.args;
  if (!args || args.method === "create") return undefined;
  const number = args.issue_number ?? args.issueNumber;
  if (typeof args.owner !== "string" || typeof args.repo !== "string" || !Number.isSafeInteger(number) || number < 1) throw new Error("INVALID_ISSUE_MUTATION");
  return `issue:${args.owner.toLowerCase()}/${args.repo.toLowerCase()}#${number}`;
}

function rpcCall(body, name) {
  let message;
  try { message = JSON.parse(body.toString("utf8")); } catch { return undefined; }
  if (Array.isArray(message)) throw new Error("JSON_RPC_BATCH_NOT_SUPPORTED");
  if (message?.method !== "tools/call" || message?.params?.name !== name) return undefined;
  if (message.jsonrpc !== "2.0" || !(typeof message.id === "string" || typeof message.id === "number")) throw new Error("INVALID_TOOL_CALL");
  return { args: message.params.arguments, id: message.id };
}

function toolsListCall(body) {
  let message;
  try { message = JSON.parse(body.toString("utf8")); } catch { return undefined; }
  return message?.jsonrpc === "2.0" && message.method === "tools/list" ? message : undefined;
}

function mcpResult(id, value, isError = false) {
  return Buffer.from(JSON.stringify({ jsonrpc: "2.0", id, result: { content: [{ type: "text", text: JSON.stringify(value) }], ...(isError ? { isError: true } : {}) } }));
}

function rpcEnvelope(bytes) {
  let text = bytes.toString("utf8");
  if (text.trimStart().startsWith("event:") || text.trimStart().startsWith("data:")) {
    const data = text.split(/\r?\n/).filter((line) => line.startsWith("data:"));
    if (data.length !== 1) throw new Error("INVALID_MCP_RESULT");
    text = data[0].slice(5).trim();
  }
  try { return JSON.parse(text); } catch { throw new Error("INVALID_MCP_RESULT"); }
}

const workspacePushTool = Object.freeze({
  name: "push_workspace_files",
  description: "Commit selected files from the current workspace to an existing GitHub branch. The branch must still point to the trusted workspace base commit.",
  inputSchema: {
    type: "object",
    additionalProperties: false,
    required: ["owner", "repo", "branch", "message", "paths"],
    properties: {
      owner: { type: "string", minLength: 1 },
      repo: { type: "string", minLength: 1 },
      branch: { type: "string", minLength: 1 },
      message: { type: "string", minLength: 1, maxLength: 256 },
      paths: { type: "array", minItems: 1, maxItems: 32, uniqueItems: true, items: { type: "string", minLength: 1, maxLength: 512 } },
    },
  },
});
const issueLifecyclesTool = Object.freeze({
  name: "dispatch_issue_lifecycles",
  description: "List the latest durable Dispatch developer lifecycle for each issue in this repository. Use this before recovering issue routing.",
  inputSchema: { type: "object", additionalProperties: false, properties: {} },
});
const labelPullRequestTool = Object.freeze({
  name: "label_pull_request_for_review",
  description: "Apply the review routing label to one verified open pull request.",
  inputSchema: {
    type: "object", additionalProperties: false, required: ["owner", "repo", "pullNumber"],
    properties: { owner: { type: "string", minLength: 1 }, repo: { type: "string", minLength: 1 }, pullNumber: { type: "integer", minimum: 1 } },
  },
});

async function executeWorkspacePush(call, config, provider) {
  if (!config.workspace || !config.permissions || config.permissions.contents !== "write") throw new Error("WORKSPACE_PUSH_UNAVAILABLE");
  const args = call.args;
  if (!args || typeof args !== "object" || Array.isArray(args)
    || typeof args.owner !== "string" || !/^[A-Za-z0-9-]{1,39}$/.test(args.owner)
    || typeof args.repo !== "string" || !/^[A-Za-z0-9_.-]+$/.test(args.repo)
    || typeof args.branch !== "string" || !/^(?!\/|.*(?:\.\.|\/\.|\.lock(?:\/|$)))[A-Za-z0-9._\/-]{1,255}$/.test(args.branch)
    || typeof args.message !== "string" || args.message.length < 1 || args.message.length > 256
    || !Array.isArray(args.paths) || args.paths.length < 1 || args.paths.length > 32 || new Set(args.paths).size !== args.paths.length) {
    throw new Error("INVALID_WORKSPACE_PUSH");
  }
  const root = await realpath(config.workspace.directory);
  const files = [];
  let total = 0;
  for (const path of args.paths) {
    if (typeof path !== "string" || path.length > 512 || isAbsolute(path) || path.split("/").some((part) => part === "" || part === "." || part === "..")) throw new Error("INVALID_WORKSPACE_PATH");
    const absolute = await realpath(resolve(root, path));
    const within = relative(root, absolute);
    if (within.startsWith("..") || isAbsolute(within)) throw new Error("INVALID_WORKSPACE_PATH");
    const file = await open(absolute, constants.O_RDONLY | constants.O_NOFOLLOW);
    let content;
    try {
      if (!(await file.stat()).isFile()) throw new Error("INVALID_WORKSPACE_PATH");
      content = await file.readFile();
    } finally { await file.close(); }
    total += content.length;
    if (total > MAX_WORKSPACE_PUSH_BYTES) throw new Error("WORKSPACE_PUSH_TOO_LARGE");
    files.push({ path, content: content.toString("base64") });
  }
  const token = await provider.getToken();
  const headers = { accept: "application/vnd.github+json", authorization: `Bearer ${token}`, "content-type": "application/json", "x-github-api-version": "2022-11-28" };
  const api = `https://api.github.com/repos/${encodeURIComponent(args.owner)}/${encodeURIComponent(args.repo)}`;
  const request = async (path, init = {}) => {
    const response = await fetch(`${api}${path}`, { ...init, headers: { ...headers, ...(init.headers ?? {}) }, redirect: "manual" });
    if (response.status === 401) provider.invalidate(token);
    const value = await response.json().catch(() => ({}));
    if (!response.ok) throw new Error("GITHUB_WORKSPACE_PUSH_REJECTED");
    return value;
  };
  const ref = await request(`/git/ref/heads/${args.branch.split("/").map(encodeURIComponent).join("/")}`);
  if (ref.object?.type !== "commit" || ref.object.sha !== config.workspace.baseCommit) throw new Error("WORKSPACE_BRANCH_FENCE_MISMATCH");
  const base = await request(`/git/commits/${config.workspace.baseCommit}`);
  if (typeof base.tree?.sha !== "string") throw new Error("INVALID_BASE_COMMIT");
  const entries = [];
  for (const file of files) {
    const blob = await request("/git/blobs", { method: "POST", body: JSON.stringify({ content: file.content, encoding: "base64" }) });
    if (typeof blob.sha !== "string") throw new Error("INVALID_BLOB_RESULT");
    entries.push({ path: file.path, mode: "100644", type: "blob", sha: blob.sha });
  }
  const tree = await request("/git/trees", { method: "POST", body: JSON.stringify({ base_tree: base.tree.sha, tree: entries }) });
  const commit = await request("/git/commits", { method: "POST", body: JSON.stringify({ message: args.message, tree: tree.sha, parents: [config.workspace.baseCommit] }) });
  await request(`/git/refs/heads/${args.branch.split("/").map(encodeURIComponent).join("/")}`, { method: "PATCH", body: JSON.stringify({ sha: commit.sha, force: false }) });
  return { branch: args.branch, commit: commit.sha, paths: args.paths };
}

async function executePullRequestLabel(call, config, provider) {
  const args = call.args;
  if (!config.permissions || config.permissions.issues !== "write"
    || !args || typeof args !== "object" || Array.isArray(args)
    || typeof args.owner !== "string" || !/^[A-Za-z0-9-]{1,39}$/.test(args.owner)
    || typeof args.repo !== "string" || !/^[A-Za-z0-9_.-]+$/.test(args.repo)
    || !Number.isSafeInteger(args.pullNumber) || args.pullNumber < 1) throw new Error("INVALID_PULL_REQUEST_LABEL");
  const token = await provider.getToken();
  const headers = { accept: "application/vnd.github+json", authorization: `Bearer ${token}`, "content-type": "application/json", "x-github-api-version": "2022-11-28" };
  const root = `https://api.github.com/repos/${encodeURIComponent(args.owner)}/${encodeURIComponent(args.repo)}`;
  const pull = await fetch(`${root}/pulls/${args.pullNumber}`, { headers, redirect: "manual" });
  if (pull.status === 401) provider.invalidate(token);
  const identity = await pull.json().catch(() => ({}));
  if (!pull.ok || identity.state !== "open" || identity.draft === true) throw new Error("PULL_REQUEST_NOT_REVIEWABLE");
  const labeled = await fetch(`${root}/issues/${args.pullNumber}/labels`, { method: "POST", headers, redirect: "manual", body: JSON.stringify({ labels: ["review"] }) });
  if (labeled.status === 401) provider.invalidate(token);
  if (!labeled.ok) throw new Error("PULL_REQUEST_LABEL_REJECTED");
  return { pullNumber: args.pullNumber, label: "review" };
}

async function executeFencedMerge(call, config, provider) {
  const capability = config.mergeCapability;
  if (!capability) throw new Error("MERGE_CAPABILITY_MISSING");
  const [owner, repo] = capability.repositoryFullName.split("/");
  if (call.args.owner.toLowerCase() !== owner.toLowerCase() || call.args.repo.toLowerCase() !== repo.toLowerCase()
    || call.args.pullNumber !== capability.pullRequestNumber) throw new Error("MERGE_CAPABILITY_MISMATCH");
  const token = await provider.getToken();
  const headers = { accept: "application/vnd.github+json", authorization: `Bearer ${token}`, "x-github-api-version": "2022-11-28" };
  const endpoint = `https://api.github.com/repos/${encodeURIComponent(owner)}/${encodeURIComponent(repo)}/pulls/${capability.pullRequestNumber}`;
  const current = await fetch(endpoint, { headers, redirect: "manual" });
  if (current.status === 401) provider.invalidate(token);
  if (!current.ok) return { status: 200, bytes: mcpResult(call.id, { error: "Unable to verify pull request" }, true) };
  const pullRequest = await current.json();
  if (pullRequest?.state !== "open" || pullRequest?.draft === true || pullRequest?.head?.sha !== capability.commitSha) {
    return { status: 200, bytes: mcpResult(call.id, { error: "Pull request no longer matches the approved revision" }, true) };
  }
  const merged = await fetch(`${endpoint}/merge`, { method: "PUT", headers: { ...headers, "content-type": "application/json" }, redirect: "manual",
    body: JSON.stringify({ merge_method: call.args.merge_method ?? "merge", sha: capability.commitSha }) });
  if (merged.status === 401) provider.invalidate(token);
  const result = await merged.json().catch(() => ({}));
  return { status: 200, bytes: mcpResult(call.id, merged.ok && result?.merged === true ? result : { error: "GitHub rejected the protected merge", details: result }, !(merged.ok && result?.merged === true)) };
}

async function effectRequest(config, path, body) {
  if (!config.effect) throw new Error("EFFECT_CAPABILITY_MISSING");
  const response = await fetch(new URL(path, config.effect.endpoint), {
    method: "POST", headers: { authorization: `Bearer ${config.effect.token}`, "content-type": "application/json" }, body: JSON.stringify(body), redirect: "manual",
  });
  if (!response.ok) throw new Error("EFFECT_REQUEST_REJECTED");
  return response.json();
}

async function responseBytes(upstream) {
  const bytes = Buffer.from(await upstream.arrayBuffer());
  if (bytes.length > MAX_RESPONSE_BYTES) throw new Error("RESPONSE_TOO_LARGE");
  return bytes;
}

function pullRequestIdentity(bytes, owner, repo, requestId) {
  let textBody = bytes.toString("utf8");
  if (textBody.trimStart().startsWith("event:") || textBody.trimStart().startsWith("data:")) {
    const data = textBody.split(/\r?\n/).filter((line) => line.startsWith("data:"));
    if (data.length !== 1) throw new Error("INVALID_CREATE_PULL_REQUEST_RESULT");
    textBody = data[0].slice(5).trim();
  }
  let envelope;
  try { envelope = JSON.parse(textBody); } catch { throw new Error("INVALID_CREATE_PULL_REQUEST_RESULT"); }
  if (envelope.jsonrpc !== "2.0" || envelope.id !== requestId || envelope.error || envelope.result?.isError === true || !Array.isArray(envelope.result?.content)) throw new Error("INVALID_CREATE_PULL_REQUEST_RESULT");
  const text = envelope.result.content.length === 1 && envelope.result.content[0]?.type === "text" ? envelope.result.content[0].text : undefined;
  let result;
  try { result = JSON.parse(text); } catch { throw new Error("INVALID_CREATE_PULL_REQUEST_RESULT"); }
  if (typeof result?.id !== "string" || !/^[1-9][0-9]*$/.test(result.id) || typeof result.url !== "string") throw new Error("INVALID_CREATE_PULL_REQUEST_RESULT");
  const url = new URL(result.url);
  const match = url.pathname.match(/^\/([^/]+)\/([^/]+)\/pull\/([1-9][0-9]*)$/);
  if (url.protocol !== "https:" || url.hostname.toLowerCase() !== "github.com" || url.username || url.password || url.search || url.hash
    || !match || match[1].toLowerCase() !== owner.toLowerCase() || match[2].toLowerCase() !== repo.toLowerCase()) throw new Error("INVALID_CREATE_PULL_REQUEST_RESULT");
  const number = Number(match[3]);
  if (!Number.isSafeInteger(number)) throw new Error("INVALID_CREATE_PULL_REQUEST_RESULT");
  return { githubPullRequestId: result.id, pullRequestNumber: number, pullRequestUrl: result.url };
}

export function startBroker(config, provider) {
  let issuesCreated = 0;
  let mutationSubject;
  const server = http.createServer(async (request, response) => {
    const controller = new AbortController();
    if (request.method === "GET") {
      request.once("aborted", () => controller.abort(new Error("MCP client disconnected")));
      response.once("close", () => {
        if (!response.writableFinished) controller.abort(new Error("MCP client disconnected"));
      });
    }
    try {
      if (request.url === "/livez") {
        response.writeHead(204).end();
        return;
      }
      if (request.url === "/readyz") {
        const ready = await upstreamReady(config.upstream) && await provider.getToken().then(() => true, () => false);
        response.writeHead(ready ? 204 : 503).end();
        return;
      }
      if (request.headers.authorization !== undefined || !["GET", "POST", "DELETE"].includes(request.method ?? "")) {
        response.writeHead(400).end();
        return;
      }
      const body = await readRequest(request);
      const issueSubject = request.method === "POST" ? issueMutationSubject(body) : undefined;
      if (issueSubject) {
        if (mutationSubject && mutationSubject !== issueSubject) throw new Error("MUTATION_SUBJECT_LIMIT_EXCEEDED");
        mutationSubject = issueSubject;
      }
      if (request.method === "POST" && createsIssue(body) && config.maxIssuesCreated !== undefined) {
        if (issuesCreated >= config.maxIssuesCreated) throw new Error("ISSUE_CREATE_LIMIT_EXCEEDED");
        issuesCreated += 1;
      }
      const mergeCall = request.method === "POST" ? mergePullRequestArguments(body) : undefined;
      if (mergeCall) {
        const merged = await executeFencedMerge(mergeCall, config, provider);
        response.writeHead(merged.status, { "content-type": "application/json" }).end(merged.bytes);
        return;
      }
      const workspacePushCall = request.method === "POST" ? rpcCall(body, "push_workspace_files") : undefined;
      if (workspacePushCall) {
        try {
          const pushed = await executeWorkspacePush(workspacePushCall, config, provider);
          response.writeHead(200, { "content-type": "application/json" }).end(mcpResult(workspacePushCall.id, pushed));
        } catch (error) {
          response.writeHead(200, { "content-type": "application/json" }).end(mcpResult(workspacePushCall.id, { error: error instanceof Error ? error.message : "Workspace push failed" }, true));
        }
        return;
      }
      const lifecycleCall = request.method === "POST" ? rpcCall(body, "dispatch_issue_lifecycles") : undefined;
      if (lifecycleCall) {
        try {
          if (!config.effect || !config.repositoryId) throw new Error("LIFECYCLE_QUERY_UNAVAILABLE");
          const lifecycles = await effectRequest(config, "/internal/v1/github/issue-lifecycles", {
            executionId: config.effect.executionId, repositoryId: config.repositoryId,
          });
          response.writeHead(200, { "content-type": "application/json" }).end(mcpResult(lifecycleCall.id, lifecycles));
        } catch (error) {
          response.writeHead(200, { "content-type": "application/json" }).end(mcpResult(lifecycleCall.id, { error: error instanceof Error ? error.message : "Lifecycle query failed" }, true));
        }
        return;
      }
      const labelPullRequestCall = request.method === "POST" ? rpcCall(body, "label_pull_request_for_review") : undefined;
      if (labelPullRequestCall) {
        try {
          const args = labelPullRequestCall.args;
          const subject = `pull:${String(args?.owner).toLowerCase()}/${String(args?.repo).toLowerCase()}#${String(args?.pullNumber)}`;
          if (mutationSubject && mutationSubject !== subject) throw new Error("MUTATION_SUBJECT_LIMIT_EXCEEDED");
          mutationSubject = subject;
          const labeled = await executePullRequestLabel(labelPullRequestCall, config, provider);
          response.writeHead(200, { "content-type": "application/json" }).end(mcpResult(labelPullRequestCall.id, labeled));
        } catch (error) {
          response.writeHead(200, { "content-type": "application/json" }).end(mcpResult(labelPullRequestCall.id, { error: error instanceof Error ? error.message : "PR label failed" }, true));
        }
        return;
      }
      const listCall = request.method === "POST" ? toolsListCall(body) : undefined;
      if (listCall && (config.effect || (config.workspace && config.permissions?.contents === "write") || config.permissions?.issues === "write")) {
        const upstream = await forward(request, body, config, provider, controller.signal);
        const envelope = rpcEnvelope(await responseBytes(upstream));
        if (!Array.isArray(envelope.result?.tools)) throw new Error("INVALID_TOOLS_LIST");
        if (config.workspace && config.permissions?.contents === "write") envelope.result.tools.push(workspacePushTool);
        if (config.effect) envelope.result.tools.push(issueLifecyclesTool);
        if (config.permissions?.issues === "write") envelope.result.tools.push(labelPullRequestTool);
        response.writeHead(200, { "content-type": "application/json" }).end(JSON.stringify(envelope));
        return;
      }
      const createCall = request.method === "POST" ? createPullRequestArguments(body) : undefined;
      const createArguments = createCall?.args;
      let effect;
      if (createArguments) {
        effect = await effectRequest(config, "/internal/v1/github/pull-request-effects", {
          executionId: config.effect.executionId, repositoryId: config.repositoryId, repositoryFullName: `${createArguments.owner}/${createArguments.repo}`, request: createArguments,
        });
        if (effect.created !== true) throw new Error("PULL_REQUEST_EFFECT_ALREADY_REGISTERED");
      }
      const upstream = await forward(request, body, config, provider, controller.signal);
      if (createArguments) {
        const bytes = await responseBytes(upstream);
        if (!upstream.ok) {
          response.writeHead(upstream.status, { "content-type": upstream.headers.get("content-type") ?? "application/json" }).end(bytes);
          return;
        }
        const identity = pullRequestIdentity(bytes, createArguments.owner, createArguments.repo, createCall.id);
        await effectRequest(config, `/internal/v1/github/pull-request-effects/${effect.id}/report`, { executionId: config.effect.executionId, ...identity });
        response.writeHead(upstream.status, { "content-type": upstream.headers.get("content-type") ?? "application/json" }).end(bytes);
        return;
      }
      const unbounded = request.method === "GET" && upstream.headers.get("content-type")?.startsWith("text/event-stream") === true;
      await pipeResponse(upstream, response, controller.signal, unbounded);
    } catch (error) {
      if (controller.signal.aborted) {
        if (!response.destroyed) response.destroy();
        return;
      }
      if (response.headersSent) {
        response.destroy();
        return;
      }
      response.writeHead(error instanceof Error && error.message === "REQUEST_TOO_LARGE" ? 413 : 502, { "content-type": "application/json" });
      response.end(JSON.stringify({ error: "GitHub MCP proxy request failed" }));
    }
  });
  return new Promise((resolve, reject) => {
    server.once("error", reject);
    server.listen(config.port, config.host, () => resolve({ server, close: () => new Promise((done, fail) => server.close((error) => error ? fail(error) : done())) }));
  });
}
