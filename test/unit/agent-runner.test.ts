import type { Event, OpencodeClient } from "@opencode-ai/sdk/client";
import { describe, expect, it, vi } from "vitest";
import { observeExecutionAttempt, runExecutionAttempt } from "../../src/agent/runner.js";

const endpoint = {
  directory: "/workspace",
  password: "secret",
  host: "agent.test",
  port: 4096,
  readyTimeoutMs: 1_000,
};

describe("runExecutionAttempt", () => {
  it("subscribes before prompting and returns bounded streamed output", async () => {
    const calls: string[] = [];
    const client = fakeClient([
      textEvent("session-1", "hello "),
      textEvent("session-1", "world"),
      sessionEvent("session.idle", "session-1"),
    ], calls);

    await expect(runExecutionAttempt({
      agent: "coder",
      endpoint,
      maxOutputBytes: 8,
      prompt: "do work",
      title: "attempt",
    }, client)).resolves.toEqual({ output: "hello wo", sessionId: "session-1" });
    expect(calls).toEqual(["create", "subscribe", "prompt"]);
  });

  it("reports bounded progress milestones without retaining output", async () => {
    const diagnostics: unknown[] = [];
    const client = fakeClient([
      textEvent("session-1", "untrusted assistant text"),
      sessionEvent("session.idle", "session-1"),
    ]);

    await runExecutionAttempt({
      agent: "coder",
      endpoint,
      onDiagnostic: async (diagnostic) => { diagnostics.push(diagnostic); },
      prompt: "do work",
      title: "attempt",
    }, client);

    expect(diagnostics).toEqual([
      { phase: "session_created", eventCount: 0 },
      { phase: "subscribed", eventCount: 0 },
      { phase: "prompt_submitted", eventCount: 0 },
      { phase: "prompt_submitted", eventCount: 1, lastEventType: "message.text", lastEventAt: expect.any(String) },
      { phase: "prompt_submitted", eventCount: 2, lastEventType: "session.idle", lastEventAt: expect.any(String) },
      { phase: "idle", eventCount: 2, lastEventType: "session.idle", lastEventAt: expect.any(String) },
    ]);
  });

  it("does not split a multi-byte character at the output limit", async () => {
    const client = fakeClient([
      textEvent("session-1", "a😀b"),
      sessionEvent("session.idle", "session-1"),
    ]);

    const result = await runExecutionAttempt({
      agent: "coder",
      endpoint,
      maxOutputBytes: 4,
      prompt: "do work",
      title: "attempt",
    }, client);
    expect(result.output).toBe("a");
  });

  it("completes when OpenCode reports idle through session status", async () => {
    const client = fakeClient([
      textEvent("session-1", "done"),
      sessionStatusEvent("session-1", "idle"),
    ]);

    await expect(runExecutionAttempt({
      agent: "coder",
      endpoint,
      prompt: "do work",
      title: "attempt",
    }, client)).resolves.toEqual({ output: "done", sessionId: "session-1" });
  });

  it("rejects permission requests and fails the attempt", async () => {
    const client = fakeClient([permissionEvent("session-1", "permission-1")]);

    await expect(runExecutionAttempt({
      agent: "coder",
      endpoint,
      prompt: "do work",
      title: "attempt",
    }, client)).rejects.toThrow("requested permission permission-1");
    expect(client.postSessionIdPermissionsPermissionId).toHaveBeenCalledWith(expect.objectContaining({
      body: { response: "reject" },
    }));
  });

  it("best-effort aborts the session when cancelled", async () => {
    const controller = new AbortController();
    const client = fakeClient([], [], true);
    const attempt = runExecutionAttempt({
      agent: "coder",
      endpoint,
      prompt: "do work",
      signal: controller.signal,
      title: "attempt",
    }, client);

    await vi.waitFor(() => expect(client.session.promptAsync).toHaveBeenCalled());
    controller.abort(new Error("cancelled"));

    await expect(attempt).rejects.toThrow("cancelled");
    expect(client.session.abort).toHaveBeenCalledWith({ path: { id: "session-1" } });
  });

  it("aborts a created session when cancelled during subscription", async () => {
    const controller = new AbortController();
    const client = fakeClient([]);
    vi.mocked(client.event.subscribe).mockImplementation(async () => new Promise(() => undefined));
    const attempt = runExecutionAttempt({
      agent: "coder",
      endpoint,
      prompt: "do work",
      signal: controller.signal,
      title: "attempt",
    }, client);

    await vi.waitFor(() => expect(client.event.subscribe).toHaveBeenCalled());
    controller.abort(new Error("cancelled while subscribing"));

    await expect(attempt).rejects.toThrow("cancelled while subscribing");
    expect(client.session.abort).toHaveBeenCalledWith({ path: { id: "session-1" } });
    expect(client.session.promptAsync).not.toHaveBeenCalled();
  });

  it("fails on session errors and premature stream completion", async () => {
    const errored = fakeClient([{
      type: "session.error",
      properties: { sessionID: "session-1", error: { name: "ProviderError", data: { message: "offline" } } },
    } as unknown as Event]);
    await expect(runExecutionAttempt({ agent: "coder", endpoint, prompt: "x", title: "x" }, errored))
      .rejects.toThrow("ProviderError: offline");

    const ended = fakeClient([]);
    await expect(runExecutionAttempt({ agent: "coder", endpoint, prompt: "x", title: "x" }, ended))
      .rejects.toThrow("event stream ended");
  });
});

describe("observeExecutionAttempt", () => {
  it("returns bounded messages for an already idle session without creating or prompting", async () => {
    const calls: string[] = [];
    const client = existingSessionClient({
      calls,
      messages: [userMessage("session-1"), assistantMessage("session-1", ["hello ", "😀 world"])],
      status: { "session-1": { type: "idle" } },
    });

    await expect(observeExecutionAttempt({
      endpoint,
      maxOutputBytes: 10,
      sessionId: "session-1",
    }, client)).resolves.toEqual({ output: "hello 😀", sessionId: "session-1" });
    expect(calls).toEqual(["get", "subscribe", "status", "messages"]);
    expect(client.session.create).not.toHaveBeenCalled();
    expect(client.session.promptAsync).not.toHaveBeenCalled();
  });

  it("waits for a busy session to become idle and reconstructs messages", async () => {
    const calls: string[] = [];
    const client = existingSessionClient({
      calls,
      events: [textEvent("session-1", "ignored delta"), sessionEvent("session.idle", "session-1")],
      messages: [userMessage("session-1"), assistantMessage("session-1", ["final output"])],
      status: { "session-1": { type: "busy" } },
    });

    await expect(observeExecutionAttempt({ endpoint, sessionId: "session-1" }, client))
      .resolves.toEqual({ output: "final output", sessionId: "session-1" });
    expect(calls).toEqual(["get", "subscribe", "status", "messages"]);
  });

  it("rejects an idle session without a persisted prompt exchange", async () => {
    const client = existingSessionClient({
      messages: [assistantMessage("session-1", ["orphan output"])],
      status: {},
    });

    await expect(observeExecutionAttempt({ endpoint, sessionId: "session-1" }, client))
      .rejects.toThrow(/completed prompt exchange/);
  });

  it("best-effort aborts the existing session when cancelled", async () => {
    const controller = new AbortController();
    const client = existingSessionClient({
      events: [],
      hangAfterEvents: true,
      status: { "session-1": { type: "busy" } },
    });
    const attempt = observeExecutionAttempt({
      endpoint,
      sessionId: "session-1",
      signal: controller.signal,
    }, client);

    await vi.waitFor(() => expect(client.session.status).toHaveBeenCalled());
    controller.abort(new Error("cancelled observation"));

    await expect(attempt).rejects.toThrow("cancelled observation");
    expect(client.session.abort).toHaveBeenCalledWith({ path: { id: "session-1" } });
    expect(client.session.create).not.toHaveBeenCalled();
    expect(client.session.promptAsync).not.toHaveBeenCalled();
  });
});

function fakeClient(events: Event[], calls: string[] = [], hangAfterEvents = false): OpencodeClient {
  async function* stream(): AsyncGenerator<Event> {
    for (const event of events) yield event;
    if (hangAfterEvents) await new Promise(() => undefined);
  }

  return {
    event: {
      subscribe: vi.fn(async () => {
        calls.push("subscribe");
        return { stream: stream() };
      }),
    },
    postSessionIdPermissionsPermissionId: vi.fn(async () => ({ data: true })),
    session: {
      abort: vi.fn(async () => ({ data: true })),
      create: vi.fn(async () => {
        calls.push("create");
        return { data: { id: "session-1" } };
      }),
      promptAsync: vi.fn(async () => {
        calls.push("prompt");
        return { data: undefined };
      }),
    },
  } as unknown as OpencodeClient;
}

function existingSessionClient(input: {
  calls?: string[];
  events?: Event[];
  hangAfterEvents?: boolean;
  messages?: unknown[];
  status: Record<string, { type: "idle" | "busy" | "retry" }>;
}): OpencodeClient {
  const calls = input.calls ?? [];
  async function* stream(): AsyncGenerator<Event> {
    for (const event of input.events ?? []) yield event;
    if (input.hangAfterEvents) await new Promise(() => undefined);
  }

  return {
    event: {
      subscribe: vi.fn(async () => {
        calls.push("subscribe");
        return { stream: stream() };
      }),
    },
    postSessionIdPermissionsPermissionId: vi.fn(async () => ({ data: true })),
    session: {
      abort: vi.fn(async () => ({ data: true })),
      create: vi.fn(),
      get: vi.fn(async () => {
        calls.push("get");
        return { data: { id: "session-1" } };
      }),
      messages: vi.fn(async () => {
        calls.push("messages");
        return { data: input.messages ?? [] };
      }),
      promptAsync: vi.fn(),
      status: vi.fn(async () => {
        calls.push("status");
        return { data: input.status };
      }),
    },
  } as unknown as OpencodeClient;
}

function assistantMessage(sessionID: string, texts: string[]): unknown {
  return {
    info: { role: "assistant", sessionID, time: { completed: Date.now() } },
    parts: texts.map((text, index) => ({
      id: `part-${index}`,
      messageID: "message-1",
      sessionID,
      text,
      type: "text",
    })),
  };
}

function userMessage(sessionID: string): unknown {
  return {
    info: { role: "user", sessionID },
    parts: [{ id: "prompt", messageID: "user-message", sessionID, text: "do work", type: "text" }],
  };
}

function textEvent(sessionID: string, delta: string): Event {
  return {
    type: "message.part.updated",
    properties: {
      delta,
      part: { type: "text", id: "part-1", messageID: "message-1", sessionID, text: delta },
    },
  } as Event;
}

function sessionEvent(type: "session.idle", sessionID: string): Event {
  return { type, properties: { sessionID } };
}

function sessionStatusEvent(sessionID: string, type: "idle" | "busy" | "retry"): Event {
  return { type: "session.status", properties: { sessionID, status: { type } } } as Event;
}

function permissionEvent(sessionID: string, id: string): Event {
  return {
    type: "permission.updated",
    properties: {
      id,
      messageID: "message-1",
      metadata: {},
      sessionID,
      time: { created: 0 },
      title: "Run command",
      type: "bash",
    },
  };
}
