import { generateKeyPairSync } from "node:crypto";
import http from "node:http";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import type { AddressInfo } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";

// @ts-expect-error The broker intentionally ships as dependency-free Node ESM.
import { parseStartupConfig, readGitHubAppCredentials } from "./config.mjs";
// @ts-expect-error The broker intentionally ships as dependency-free Node ESM.
import { startBroker } from "./server.mjs";
// @ts-expect-error The broker intentionally ships as dependency-free Node ESM.
import { createGitHubAppJwt, InstallationTokenProvider } from "./token.mjs";

const { privateKey } = generateKeyPairSync("rsa", { modulusLength: 2048 });
const pem = privateKey.export({ type: "pkcs8", format: "pem" }).toString();
const config = {
  tenantId: "default",
  connectionRef: "github-production",
  repositoryId: 42,
  permissions: { contents: "write", issues: "write", pull_requests: "write" },
};
const running: Array<{ close: () => Promise<void> }> = [];
const temporaryDirectories: string[] = [];

afterEach(async () => {
  await Promise.all(running.splice(0).map(({ close }) => close()));
  await Promise.all(temporaryDirectories.splice(0).map((directory) => rm(directory, { recursive: true, force: true })));
  vi.unstubAllGlobals();
});

describe("GitHub token broker", () => {
  it("validates the exact Dispatch grant and loopback upstream", () => {
    const env = {
      DISPATCH_GITHUB_TENANT: "default",
      DISPATCH_GITHUB_CONNECTION: "github-production",
      DISPATCH_GITHUB_REPOSITORY_ID: "42",
      DISPATCH_GITHUB_PERMISSIONS: "contents:write,issues:write,pull_requests:write",
      DISPATCH_CONNECTIONS: JSON.stringify({ refs: ["github-production"], schemaVersion: 1, tenantId: "default" }),
    };
    expect(parseStartupConfig(env)).toMatchObject({ ...config, upstream: "http://127.0.0.1:8082/", port: 8083 });
    expect(parseStartupConfig({ ...env, DISPATCH_GITHUB_MAX_ISSUES_CREATED: "1" }).maxIssuesCreated).toBe(1);
    expect(() => parseStartupConfig({ ...env, DISPATCH_GITHUB_MAX_ISSUES_CREATED: "0" })).toThrow(/MAX_ISSUES/);
    expect(() => parseStartupConfig({ ...env, DISPATCH_CONNECTIONS: JSON.stringify({ refs: ["other"], schemaVersion: 1, tenantId: "default" }) })).toThrow();
    expect(() => parseStartupConfig({ ...env, DISPATCH_GITHUB_MCP_UPSTREAM: "https://example.com" })).toThrow(/loopback/);
    expect(() => parseStartupConfig({ ...env, DISPATCH_GITHUB_PERMISSIONS: "contents:admin" })).toThrow();
    expect(parseStartupConfig({ ...env, DISPATCH_GITHUB_MERGE_REVIEWER_IDS: "9,10", DISPATCH_GITHUB_MERGE_CAPABILITY: JSON.stringify({ schemaVersion: 1, repositoryId: 42,
      repositoryFullName: "acme/repo", pullRequestNumber: 7, reviewerId: 9, commitSha: "a".repeat(40) }) }).mergeCapability)
      .toMatchObject({ repositoryId: 42, pullRequestNumber: 7, commitSha: "a".repeat(40) });
    expect(() => parseStartupConfig({ ...env, DISPATCH_GITHUB_MERGE_REVIEWER_IDS: "9", DISPATCH_GITHUB_MERGE_CAPABILITY: JSON.stringify({ schemaVersion: 1, repositoryId: 43,
      repositoryFullName: "acme/repo", pullRequestNumber: 7, reviewerId: 9, commitSha: "a".repeat(40) }) })).toThrow(/MERGE_CAPABILITY/);
    expect(() => parseStartupConfig({ ...env, DISPATCH_GITHUB_MERGE_REVIEWER_IDS: "10", DISPATCH_GITHUB_MERGE_CAPABILITY: JSON.stringify({ schemaVersion: 1, repositoryId: 42,
      repositoryFullName: "acme/repo", pullRequestNumber: 7, reviewerId: 9, commitSha: "a".repeat(40) }) })).toThrow(/MERGE_CAPABILITY/);
    expect(() => parseStartupConfig({ ...env, DISPATCH_GITHUB_MERGE_REVIEWER_IDS: "9,9", DISPATCH_GITHUB_MERGE_CAPABILITY: JSON.stringify({ schemaVersion: 1, repositoryId: 42,
      repositoryFullName: "acme/repo", pullRequestNumber: 7, reviewerId: 9, commitSha: "a".repeat(40) }) })).toThrow(/REVIEWER_IDS/);
  });

  it("validates mounted GitHub App credentials", async () => {
    const values: Record<string, string> = { app: "10\n", installation: "20\n", key: pem };
    await expect(readGitHubAppCredentials(
      { appId: "app", installationId: "installation", privateKey: "key" },
      async (path: string) => values[path],
    )).resolves.toMatchObject({ appId: 10, installationId: 20, privateKey: pem });
    await expect(readGitHubAppCredentials(
      { appId: "app", installationId: "installation", privateKey: "bad" },
      async (path: string) => path === "bad" ? "not a key" : values[path],
    )).rejects.toThrow(/private key/);
  });

  it("mints, scopes, caches, and refreshes installation tokens", async () => {
    let sequence = 0;
    const readCredentials = vi.fn(async () => ({ appId: 10, installationId: 20, privateKey: pem }));
    const fetch = vi.fn(async (_url: string, init: RequestInit) => {
      sequence += 1;
      expect(init.body).toBe(JSON.stringify({ repository_ids: [42], permissions: config.permissions }));
      expect((init.headers as Record<string, string>).Authorization).toMatch(/^Bearer eyJ/);
      return Response.json({
        token: `ghs_token_${sequence}`,
        expires_at: "2026-07-19T02:00:00Z",
        repository_selection: "selected",
        repositories: [{ id: 42 }],
        permissions: { ...config.permissions, metadata: "read" },
      });
    });
    const provider = new InstallationTokenProvider(config, readCredentials, {
      fetch,
      now: () => Date.parse("2026-07-19T01:00:00Z"),
    });
    expect(await provider.getToken()).toBe("ghs_token_1");
    expect(await provider.getToken()).toBe("ghs_token_1");
    expect(fetch).toHaveBeenCalledTimes(1);
    expect(readCredentials).toHaveBeenCalledTimes(1);
    provider.invalidate("ghs_token_1");
    expect(await provider.getToken()).toBe("ghs_token_2");
    expect(readCredentials).toHaveBeenCalledTimes(2);
  });

  it("rejects broader token permissions", async () => {
    const provider = new InstallationTokenProvider(config, { appId: 10, installationId: 20, privateKey: pem }, {
      fetch: async () => Response.json({
        token: "ghs_token",
        expires_at: "2026-07-19T02:00:00Z",
        repositories: [{ id: 42 }],
        permissions: { ...config.permissions, workflows: "write" },
      }),
      now: () => Date.parse("2026-07-19T01:00:00Z"),
    });
    await expect(provider.getToken()).rejects.toThrow(/permissions mismatch/);
  });

  it("injects tokens, strips caller authorization, and never replays a 401 request", async () => {
    const seen: Array<string | undefined> = [];
    const upstream = await listen(http.createServer((request, response) => {
      seen.push(request.headers.authorization);
      if (seen.length === 1) response.writeHead(401).end();
      else response.writeHead(200, { "content-type": "application/json" }).end('{"ok":true}');
    }));
    running.push(upstream);
    let token = 0;
    const provider = {
      getToken: async () => `ghs_${++token}`,
      invalidate: vi.fn(),
    };
    const broker = await startBroker({ upstream: `http://127.0.0.1:${upstream.port}/`, host: "127.0.0.1", port: 0 }, provider);
    running.push(broker);
    const port = (broker.server.address() as AddressInfo).port;
    const response = await fetch(`http://127.0.0.1:${port}/`, {
      method: "POST",
      headers: { authorization: "Bearer attacker", "content-type": "application/json" },
      body: "{}",
    });
    expect(response.status).toBe(400);

    const accepted = await fetch(`http://127.0.0.1:${port}/`, { method: "POST", headers: { "content-type": "application/json" }, body: "{}" });
    expect(accepted.status).toBe(401);
    const retried = await fetch(`http://127.0.0.1:${port}/`, { method: "POST", headers: { "content-type": "application/json" }, body: "{}" });
    expect(retried.status).toBe(200);
    expect(seen).toEqual(["Bearer ghs_1", "Bearer ghs_2"]);
    expect(provider.invalidate).toHaveBeenCalledWith("ghs_1");
  });

  it("forwards only the fixed MCP root endpoint", async () => {
    let upstreamCalls = 0;
    const upstream = await listen(http.createServer((_request, response) => {
      upstreamCalls += 1;
      response.writeHead(200).end();
    }));
    running.push(upstream);
    const broker = await startBroker(
      { upstream: `http://127.0.0.1:${upstream.port}/`, host: "127.0.0.1", port: 0 },
      { getToken: async () => "ghs", invalidate: () => {} },
    );
    running.push(broker);
    const port = (broker.server.address() as AddressInfo).port;

    expect((await fetch(`http://127.0.0.1:${port}/other`)).status).toBe(502);
    expect((await fetch(`http://127.0.0.1:${port}//127.0.0.1:1/`)).status).toBe(502);
    expect(upstreamCalls).toBe(0);
  });

  it("creates a signed GitHub App JWT", () => {
    const jwt = createGitHubAppJwt({ appId: 10, privateKey: pem, now: () => Date.parse("2026-07-19T01:00:00Z") });
    expect(jwt.split(".")).toHaveLength(3);
  });

  it("enforces an execution-scoped issue creation budget", async () => {
    let upstreamCalls = 0;
    const upstream = await listen(http.createServer((_request, response) => {
      upstreamCalls += 1;
      response.writeHead(200, { "content-type": "application/json" }).end("{}");
    }));
    running.push(upstream);
    const broker = await startBroker({ upstream: `http://127.0.0.1:${upstream.port}/`, host: "127.0.0.1", port: 0, maxIssuesCreated: 1 },
      { getToken: async () => "ghs", invalidate: () => {} });
    running.push(broker);
    const endpoint = `http://127.0.0.1:${(broker.server.address() as AddressInfo).port}/`;
    const create = JSON.stringify({ jsonrpc: "2.0", id: 1, method: "tools/call", params: { name: "issue_write", arguments: { method: "create", owner: "acme", repo: "repo", title: "Bug", body: "Evidence" } } });
    expect((await fetch(endpoint, { method: "POST", body: create })).status).toBe(200);
    expect((await fetch(endpoint, { method: "POST", body: create })).status).toBe(502);
    expect(upstreamCalls).toBe(1);
  });

  it("limits maintenance mutations to one issue subject", async () => {
    let upstreamCalls = 0;
    const upstream = await listen(http.createServer((_request, response) => {
      upstreamCalls += 1;
      response.writeHead(200, { "content-type": "application/json" }).end("{}");
    }));
    running.push(upstream);
    const broker = await startBroker({ upstream: `http://127.0.0.1:${upstream.port}/`, host: "127.0.0.1", port: 0 },
      { getToken: async () => "ghs", invalidate: () => {} });
    running.push(broker);
    const endpoint = `http://127.0.0.1:${(broker.server.address() as AddressInfo).port}/`;
    const update = (issue_number: number) => fetch(endpoint, { method: "POST", body: JSON.stringify({ jsonrpc: "2.0", id: issue_number, method: "tools/call", params: {
      name: "issue_write", arguments: { method: "update", owner: "acme", repo: "repo", issue_number, labels: ["medium"] },
    } }) });
    expect((await update(7)).status).toBe(200);
    expect((await update(7)).status).toBe(200);
    expect((await update(8)).status).toBe(502);
    expect(upstreamCalls).toBe(2);
  });

  it("returns a structured MCP error when PR review labeling is rejected", async () => {
    const originalFetch = globalThis.fetch;
    vi.stubGlobal("fetch", vi.fn(async (input: string | URL | Request, init?: RequestInit) => {
      if (String(input).startsWith("https://api.github.com/")) return Response.json({ state: "closed" });
      return originalFetch(input, init);
    }));
    const broker = await startBroker({ ...config, upstream: "http://127.0.0.1:1/", host: "127.0.0.1", port: 0 },
      { getToken: async () => "ghs", invalidate: () => {} });
    running.push(broker);
    const response = await originalFetch(`http://127.0.0.1:${(broker.server.address() as AddressInfo).port}/`, { method: "POST", body: JSON.stringify({
      jsonrpc: "2.0", id: 1, method: "tools/call", params: { name: "label_pull_request_for_review", arguments: { owner: "acme", repo: "repo", pullNumber: 7 } },
    }) });
    expect(response.status).toBe(200);
    await expect(response.json()).resolves.toMatchObject({ result: { isError: true } });
  });

  it("streams MCP SSE responses", async () => {
    const upstream = await listen(http.createServer((_request, response) => {
      response.writeHead(200, { "content-type": "text/event-stream" });
      response.write("event: message\n");
      response.end("data: {\"jsonrpc\":\"2.0\"}\n\n");
    }));
    running.push(upstream);
    const broker = await startBroker(
      { upstream: `http://127.0.0.1:${upstream.port}/`, host: "127.0.0.1", port: 0 },
      { getToken: async () => "ghs_token", invalidate: () => {} },
    );
    running.push(broker);
    const port = (broker.server.address() as AddressInfo).port;
    const response = await fetch(`http://127.0.0.1:${port}/`, { method: "POST", body: "{}" });
    expect(response.headers.get("content-type")).toBe("text/event-stream");
    await expect(response.text()).resolves.toBe("event: message\ndata: {\"jsonrpc\":\"2.0\"}\n\n");
  });

  it("cancels the upstream stream when the MCP client disconnects", async () => {
    let upstreamClosed!: () => void;
    const closed = new Promise<void>((resolve) => { upstreamClosed = resolve; });
    const upstream = await listen(http.createServer((_request, response) => {
      response.writeHead(200, { "content-type": "text/event-stream" });
      response.write("event: message\ndata: {}\n\n");
      response.once("close", upstreamClosed);
    }));
    running.push(upstream);
    const broker = await startBroker(
      { upstream: `http://127.0.0.1:${upstream.port}/`, host: "127.0.0.1", port: 0 },
      { getToken: async () => "ghs_token", invalidate: () => {} },
    );
    running.push(broker);
    const request = http.request({ host: "127.0.0.1", port: (broker.server.address() as AddressInfo).port, method: "GET" });
    request.end();
    await new Promise<void>((resolve) => request.once("response", (response) => {
      response.once("data", () => {
        response.destroy();
        resolve();
      });
    }));
    await expect(Promise.race([closed, new Promise((_, reject) => setTimeout(() => reject(new Error("upstream remained open")), 2_000))])).resolves.toBeUndefined();
  });

  it("does not cancel a POST tool invocation when the MCP client disconnects", async () => {
    let receive!: () => void;
    const received = new Promise<void>((resolve) => { receive = resolve; });
    let complete!: () => void;
    const completed = new Promise<void>((resolve) => { complete = resolve; });
    let upstreamAborted = false;
    const upstream = await listen(http.createServer(async (upstreamRequest, response) => {
      receive();
      upstreamRequest.once("aborted", () => { upstreamAborted = true; });
      await new Promise((resolve) => setTimeout(resolve, 100));
      response.writeHead(200, { "content-type": "application/json" }).end("{}");
      complete();
    }));
    running.push(upstream);
    const broker = await startBroker(
      { upstream: `http://127.0.0.1:${upstream.port}/`, host: "127.0.0.1", port: 0 },
      { getToken: async () => "ghs_token", invalidate: () => {} },
    );
    running.push(broker);
    const request = http.request({ host: "127.0.0.1", port: (broker.server.address() as AddressInfo).port, method: "POST" });
    request.on("error", () => {});
    request.end("{}");
    await received;
    request.destroy();
    await expect(completed).resolves.toBeUndefined();
    expect(upstreamAborted).toBe(false);
  });

  it("registers and reports create_pull_request before exposing success", async () => {
    const callbacks: Array<{ url: string; body: any; authorization: string | undefined }> = [];
    const control = await listen(http.createServer(async (request, response) => {
      const chunks = [];
      for await (const chunk of request) chunks.push(chunk);
      callbacks.push({ url: request.url!, body: JSON.parse(Buffer.concat(chunks).toString()), authorization: request.headers.authorization });
      response.writeHead(200, { "content-type": "application/json" }).end(callbacks.length === 1 ? '{"created":true,"id":"effect-1","state":"REGISTERED"}' : '{"id":"effect-1","state":"REPORTED"}');
    }));
    running.push(control);
    const upstream = await listen(http.createServer((_request, response) => response.writeHead(200, { "content-type": "text/event-stream" }).end(`event: message\ndata: ${JSON.stringify({
      jsonrpc: "2.0", id: 1, result: { content: [{ type: "text", text: '{"id":"9001","url":"https://github.com/acme/repo/pull/42"}' }] },
    })}\n\n`)));
    running.push(upstream);
    const broker = await startBroker({ upstream: `http://127.0.0.1:${upstream.port}/`, host: "127.0.0.1", port: 0, repositoryId: 7,
      effect: { endpoint: `http://127.0.0.1:${control.port}/`, executionId: "execution-1", token: "fence" } }, { getToken: async () => "ghs", invalidate: () => {} });
    running.push(broker);
    const response = await fetch(`http://127.0.0.1:${(broker.server.address() as AddressInfo).port}/`, { method: "POST", body: JSON.stringify({
      jsonrpc: "2.0", id: 1, method: "tools/call", params: { name: "create_pull_request", arguments: { owner: "acme", repo: "repo", title: "PR", head: "feature", base: "main" } },
    }) });
    expect(response.status).toBe(200);
    expect(callbacks).toEqual([
      expect.objectContaining({ url: "/internal/v1/github/pull-request-effects", authorization: "Bearer fence", body: expect.objectContaining({ executionId: "execution-1", repositoryId: 7, repositoryFullName: "acme/repo" }) }),
      expect.objectContaining({ url: "/internal/v1/github/pull-request-effects/effect-1/report", body: expect.objectContaining({ githubPullRequestId: "9001", pullRequestNumber: 42 }) }),
    ]);
  });

  it("exposes and executes a fenced workspace file push", async () => {
    const directory = await mkdtemp(join(tmpdir(), "dispatch-broker-"));
    temporaryDirectories.push(directory);
    await writeFile(join(directory, "large.ts"), "export const fixed = true;\n");
    const upstream = await listen(http.createServer(async (request, response) => {
      const chunks = [];
      for await (const chunk of request) chunks.push(chunk);
      const body = JSON.parse(Buffer.concat(chunks).toString());
      response.writeHead(200, { "content-type": "application/json" }).end(JSON.stringify({ jsonrpc: "2.0", id: body.id, result: { tools: [] } }));
    }));
    running.push(upstream);
    const originalFetch = globalThis.fetch;
    const requests: Array<{ method: string; path: string; body?: any }> = [];
    vi.stubGlobal("fetch", vi.fn(async (input: string | URL | Request, init?: RequestInit) => {
      const url = new URL(String(input));
      if (url.hostname !== "api.github.com") return originalFetch(input, init);
      const body = init?.body ? JSON.parse(String(init.body)) : undefined;
      requests.push({ method: init?.method ?? "GET", path: url.pathname, body });
      if (url.pathname.includes("/git/ref/heads/")) return Response.json({ object: { type: "commit", sha: "a".repeat(40) } });
      if (url.pathname.endsWith(`/git/commits/${"a".repeat(40)}`)) return Response.json({ tree: { sha: "base-tree" } });
      if (url.pathname.endsWith("/git/blobs")) return Response.json({ sha: "blob-sha" });
      if (url.pathname.endsWith("/git/trees")) return Response.json({ sha: "tree-sha" });
      if (url.pathname.endsWith("/git/commits")) return Response.json({ sha: "commit-sha" });
      return Response.json({ ok: true });
    }));
    const broker = await startBroker({ ...config, upstream: `http://127.0.0.1:${upstream.port}/`, host: "127.0.0.1", port: 0,
      workspace: { baseCommit: "a".repeat(40), directory } }, { getToken: async () => "ghs", invalidate: () => {} });
    running.push(broker);
    const endpoint = `http://127.0.0.1:${(broker.server.address() as AddressInfo).port}/`;
    const tools = await fetch(endpoint, { method: "POST", body: JSON.stringify({ jsonrpc: "2.0", id: 1, method: "tools/list", params: {} }) }).then((response) => response.json()) as any;
    expect(tools.result.tools.map((tool: { name: string }) => tool.name)).toContain("push_workspace_files");
    const pushed = await fetch(endpoint, { method: "POST", body: JSON.stringify({ jsonrpc: "2.0", id: 2, method: "tools/call", params: {
      name: "push_workspace_files", arguments: { owner: "acme", repo: "repo", branch: "fix/large", message: "fix: update large file", paths: ["large.ts"] },
    } }) }).then((response) => response.json()) as any;
    expect(pushed.result.isError).not.toBe(true);
    expect(requests).toEqual(expect.arrayContaining([
      expect.objectContaining({ method: "POST", path: "/repos/acme/repo/git/blobs", body: { content: Buffer.from("export const fixed = true;\n").toString("base64"), encoding: "base64" } }),
      expect.objectContaining({ method: "PATCH", path: "/repos/acme/repo/git/refs/heads/fix/large", body: { sha: "commit-sha", force: false } }),
    ]));
  });

  it("parses an SSE create_pull_request result and refuses duplicate mutation registration", async () => {
    let upstreamCalls = 0;
    const control = await listen(http.createServer((_request, response) => response.writeHead(200, { "content-type": "application/json" }).end('{"created":false,"id":"effect-1","state":"REGISTERED"}')));
    running.push(control);
    const upstream = await listen(http.createServer((_request, response) => { upstreamCalls += 1; response.writeHead(200).end(); }));
    running.push(upstream);
    const broker = await startBroker({ upstream: `http://127.0.0.1:${upstream.port}/`, host: "127.0.0.1", port: 0, repositoryId: 7,
      effect: { endpoint: `http://127.0.0.1:${control.port}/`, executionId: "execution-1", token: "fence" } }, { getToken: async () => "ghs", invalidate: () => {} });
    running.push(broker);
    const response = await fetch(`http://127.0.0.1:${(broker.server.address() as AddressInfo).port}/`, { method: "POST", body: JSON.stringify({
      jsonrpc: "2.0", id: 1, method: "tools/call", params: { name: "create_pull_request", arguments: { owner: "acme", repo: "repo", title: "PR", head: "feature", base: "main" } },
    }) });
    expect(response.status).toBe(502);
    expect(upstreamCalls).toBe(0);
  });

  it("rejects JSON-RPC batches instead of bypassing effect interception", async () => {
    let upstreamCalls = 0;
    const upstream = await listen(http.createServer((_request, response) => { upstreamCalls += 1; response.writeHead(200).end(); }));
    running.push(upstream);
    const broker = await startBroker({ upstream: `http://127.0.0.1:${upstream.port}/`, host: "127.0.0.1", port: 0 }, { getToken: async () => "ghs", invalidate: () => {} });
    running.push(broker);
    const response = await fetch(`http://127.0.0.1:${(broker.server.address() as AddressInfo).port}/`, { method: "POST", body: JSON.stringify([
      { jsonrpc: "2.0", id: 1, method: "tools/call", params: { name: "create_pull_request", arguments: {} } },
    ]) });
    expect(response.status).toBe(502);
    expect(upstreamCalls).toBe(0);
  });

  it("rejects merge calls outside the execution-scoped capability", async () => {
    let upstreamCalls = 0;
    const upstream = await listen(http.createServer((_request, response) => { upstreamCalls += 1; response.writeHead(200).end(); }));
    running.push(upstream);
    const broker = await startBroker({ upstream: `http://127.0.0.1:${upstream.port}/`, host: "127.0.0.1", port: 0,
      mergeCapability: { schemaVersion: 1, repositoryId: 7, repositoryFullName: "acme/repo", pullRequestNumber: 42, reviewerId: 9, commitSha: "a".repeat(40) } },
    { getToken: async () => "ghs", invalidate: () => {} });
    running.push(broker);
    const response = await fetch(`http://127.0.0.1:${(broker.server.address() as AddressInfo).port}/`, { method: "POST", body: JSON.stringify({
      jsonrpc: "2.0", id: 1, method: "tools/call", params: { name: "merge_pull_request", arguments: { owner: "acme", repo: "repo", pullNumber: 41 } },
    }) });
    expect(response.status).toBe(502);
    expect(upstreamCalls).toBe(0);
  });

  it("merges only the capability PR at the approved SHA", async () => {
    const originalFetch = globalThis.fetch;
    const requests: Array<{ body?: string; method?: string; url: string }> = [];
    vi.stubGlobal("fetch", vi.fn(async (input: string | URL | Request, init?: RequestInit) => {
      const url = String(input);
      if (!url.startsWith("https://api.github.com/")) return originalFetch(input, init);
      requests.push({ url, method: init?.method, body: init?.body as string | undefined });
      if (init?.method === "PUT") return Response.json({ merged: true, message: "Pull Request successfully merged" });
      return Response.json({ state: "open", draft: false, head: { sha: "a".repeat(40) } });
    }));
    const upstream = await listen(http.createServer((_request, response) => response.writeHead(500).end()));
    running.push(upstream);
    const broker = await startBroker({ upstream: `http://127.0.0.1:${upstream.port}/`, host: "127.0.0.1", port: 0,
      mergeCapability: { schemaVersion: 1, repositoryId: 7, repositoryFullName: "acme/repo", pullRequestNumber: 42, reviewerId: 9, commitSha: "a".repeat(40) } },
    { getToken: async () => "ghs", invalidate: () => {} });
    running.push(broker);
    const response = await originalFetch(`http://127.0.0.1:${(broker.server.address() as AddressInfo).port}/`, { method: "POST", body: JSON.stringify({
      jsonrpc: "2.0", id: 1, method: "tools/call", params: { name: "merge_pull_request", arguments: { owner: "acme", repo: "repo", pullNumber: 42, merge_method: "squash" } },
    }) });
    expect(response.status).toBe(200);
    expect(await response.json()).toMatchObject({ result: { content: [{ type: "text" }] } });
    expect(requests).toEqual([
      expect.objectContaining({ url: "https://api.github.com/repos/acme/repo/pulls/42" }),
      expect.objectContaining({ url: "https://api.github.com/repos/acme/repo/pulls/42/merge", method: "PUT", body: JSON.stringify({ merge_method: "squash", sha: "a".repeat(40) }) }),
    ]);
  });
});

async function listen(server: http.Server): Promise<{ server: http.Server; port: number; close: () => Promise<void> }> {
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  return {
    server,
    port: (server.address() as AddressInfo).port,
    close: () => new Promise((resolve, reject) => server.close((error) => error ? reject(error) : resolve())),
  };
}
