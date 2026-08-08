import { Buffer } from "node:buffer";
import { createOpencodeClient, type OpencodeClient } from "@opencode-ai/sdk/client";

export type AgentEndpoint = {
  password: string;
  host: string;
};

export type OpenCodeConnectionOptions = {
  directory: string;
  port: number;
  readyTimeoutMs: number;
};

export type OpenCodeEndpoint = AgentEndpoint & OpenCodeConnectionOptions;

type ExistingConfigShape = {
  claimReadyTimeoutMs: number;
  opencodeDirectory: string;
  opencodePort: number;
};

export function createAgentClient(
  endpoint: AgentEndpoint,
  options: OpenCodeConnectionOptions | ExistingConfigShape,
): OpencodeClient {
  const connection = normalizeOptions(options);
  return createOpencodeClient({
    baseUrl: baseUrl(endpoint.host, connection.port),
    directory: connection.directory,
    headers: authHeaders(endpoint.password),
  });
}

export async function waitForOpencodeReady(
  endpoint: AgentEndpoint,
  options: OpenCodeConnectionOptions | ExistingConfigShape,
  signal?: AbortSignal,
): Promise<void> {
  const connection = normalizeOptions(options);
  const deadline = Date.now() + connection.readyTimeoutMs;
  const url = `${baseUrl(endpoint.host, connection.port)}/global/health`;

  while (Date.now() <= deadline) {
    signal?.throwIfAborted();
    try {
      const remainingMs = deadline - Date.now();
      if (remainingMs <= 0) break;
      // A Pod can accept a TCP connection before OpenCode is ready to answer it.
      // Bound each probe so one half-open request cannot consume the whole attempt.
      const response = await fetch(url, {
        headers: authHeaders(endpoint.password),
        signal: signal ? AbortSignal.any([signal, AbortSignal.timeout(Math.min(2_000, remainingMs))]) : AbortSignal.timeout(Math.min(2_000, remainingMs)),
      });
      if (response.ok) return;
    } catch (error) {
      signal?.throwIfAborted();
      // The sandbox Pod can be Ready before opencode has bound its HTTP port.
      void error;
    }

    const remainingMs = deadline - Date.now();
    if (remainingMs <= 0) break;
    await sleep(Math.min(500, remainingMs), signal);
  }

  throw new Error(`opencode server at ${url} did not become ready`);
}

function normalizeOptions(options: OpenCodeConnectionOptions | ExistingConfigShape): OpenCodeConnectionOptions {
  if ("port" in options) return options;
  return {
    directory: options.opencodeDirectory,
    port: options.opencodePort,
    readyTimeoutMs: options.claimReadyTimeoutMs,
  };
}

function baseUrl(host: string, port: number): string {
  return `http://${formatHost(host)}:${port}`;
}

function formatHost(host: string): string {
  if (host.startsWith("[") || !host.includes(":")) return host;
  return `[${host}]`;
}

function authHeaders(password: string): Record<string, string> {
  const token = Buffer.from(`opencode:${password}`).toString("base64");
  return { Authorization: `Basic ${token}` };
}

function sleep(ms: number, signal?: AbortSignal): Promise<void> {
  return new Promise((resolve, reject) => {
    const finish = () => {
      signal?.removeEventListener("abort", abort);
      resolve();
    };
    const timer = setTimeout(finish, ms);
    if (!signal) return;

    const abort = () => {
      clearTimeout(timer);
      signal.removeEventListener("abort", abort);
      reject(signal.reason ?? new DOMException("The operation was aborted", "AbortError"));
    };
    if (signal.aborted) abort();
    else signal.addEventListener("abort", abort, { once: true });
  });
}
