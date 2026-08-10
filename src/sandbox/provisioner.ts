import { createHash, randomBytes } from "node:crypto";
import { CustomObjectsApi, KubeConfig, PatchStrategy, Watch, setHeaderOptions } from "@kubernetes/client-node";
import { buildOpencodeConfigContent } from "../agent/config.js";
import type { Config } from "../config.js";
import type { RequestedCancellationCleanup } from "../dispatch/types.js";
import { logger } from "../logger.js";
import { sandboxClaims } from "../observability/metrics.js";
import { claimNameForExecutionAttempt } from "./naming.js";
import type {
  ExecutionAttemptEndpoint,
  ExecutionAttemptProvisioner,
  ExecutionAttemptProvisioningInput,
  SandboxClaim,
  SandboxEnvVar,
} from "./types.js";

const GROUP = "extensions.agents.x-k8s.io";
const PLURAL = "sandboxclaims";
const CONNECTIONS_DIGEST_ANNOTATION = "dispatch.dev/connections-digest";
const FENCING_TOKEN_ANNOTATION = "dispatch.dev/fencing-token";
const TERMINAL_CLAIM_REASONS = new Set([
  "EnvVarsInjectionRejected",
  "InvalidMetadata",
  "ReconcilerError",
  "TemplateNotFound",
  "WarmPoolNotFound",
]);

export class SandboxClaimRejectedError extends Error {
  constructor(claimName: string, reason: string) {
    super(`SandboxClaim ${claimName} was rejected: ${reason}`);
    this.name = "SandboxClaimRejectedError";
  }
}

export class SandboxClaimCleanupError extends Error {
  constructor(options: ErrorOptions) {
    super("Failed to clean up sandbox claim after provisioning error", options);
    this.name = "SandboxClaimCleanupError";
  }
}

export class SandboxClaimExecutionAttemptProvisioner implements ExecutionAttemptProvisioner {
  private readonly api: CustomObjectsApi;
  private readonly watch: Watch;

  constructor(kubeConfig: KubeConfig, private readonly config: Config) {
    this.api = kubeConfig.makeApiClient(CustomObjectsApi);
    this.watch = new Watch(kubeConfig);
  }

  async provision(input: ExecutionAttemptProvisioningInput, signal: AbortSignal): Promise<ExecutionAttemptEndpoint> {
    throwIfAborted(signal);
    if (this.config.sandboxClaimApiVersion === "v1beta1" && (!input.warmPool || input.warmPool === "none" || input.warmPool === "default")) {
      throw new Error("v1beta1 SandboxClaims require a concrete SandboxWarmPool name");
    }
    if (this.config.sandboxClaimApiVersion === "v1alpha1" && input.workspace.type === "git" && input.warmPool && input.warmPool !== "none") {
      throw new Error("Git workspaces cannot be provisioned from a warm pool");
    }
    if (this.config.sandboxClaimApiVersion === "v1alpha1" && input.connections.length > 0 && input.warmPool !== "none") {
      throw new Error("Connection authorization cannot be provisioned from a warm pool");
    }
    if (input.connections.some(({ sidecar }) => sidecar === "opencode")) {
      throw new Error("Connection authorization cannot target the opencode container");
    }
    const claimName = claimNameForExecutionAttempt(input.executionId, input.attempt);
    const ownership = ownershipAnnotations(input);
    const log = logger.child({ executionId: input.executionId, attempt: input.attempt, claimName });
    let claim = await this.getClaim(claimName);

    if (claim) {
      assertOwnership(claim, ownership);
      log.info("observing existing sandbox claim");
    } else {
      const password = randomBytes(24).toString("base64url");
      log.info("creating sandbox claim");
      try {
        claim = (await this.api.createNamespacedCustomObject({
          group: GROUP,
          version: this.config.sandboxClaimApiVersion,
          namespace: this.config.kubeNamespace,
          plural: PLURAL,
          body: this.buildClaim(input, claimName, password, ownership),
        })) as SandboxClaim;
        sandboxClaims.inc({ tenant: input.tenantId, profile_id: input.profileVersion.profileId, operation: "created", result: "succeeded" });
      } catch (error) {
        if (!isConflict(error)) {
          sandboxClaims.inc({ tenant: input.tenantId, profile_id: input.profileVersion.profileId, operation: "created", result: "failed" });
          throw error;
        }
        claim = await this.getClaim(claimName);
        if (!claim) throw error;
        assertOwnership(claim, ownership);
      }
    }

    const password = passwordFromClaim(claim);
    try {
      const endpoint = await this.waitForReady(claim, password, ownership, signal, log);
      return { ...endpoint, release: input };
    } catch (error) {
      try {
        await this.release(input, AbortSignal.timeout(Math.min(this.config.claimReadyTimeoutMs, 10_000)));
      } catch (cleanupError) {
        log.error("failed to clean up sandbox claim after provisioning error", { error: String(cleanupError) });
        throw new SandboxClaimCleanupError({ cause: cleanupError });
      }
      throw error;
    }
  }

  async adopt(
    input: ExecutionAttemptProvisioningInput,
    expectedWorkloadName: string,
    signal: AbortSignal,
  ): Promise<ExecutionAttemptEndpoint> {
    throwIfAborted(signal);
    const claimName = claimNameForExecutionAttempt(input.executionId, input.attempt);
    if (claimName !== expectedWorkloadName) {
      throw new Error(`Adoption workload ${expectedWorkloadName} does not match expected claim ${claimName}`);
    }
    let claim = await this.getClaim(claimName);
    if (!claim) throw new Error(`SandboxClaim ${claimName} was not found for adoption`);

    const ownership = ownershipAnnotations(input);
    assertOwnership(claim, ownership, new Set([FENCING_TOKEN_ANNOTATION]));
    const previousFence = claim.metadata.annotations?.[FENCING_TOKEN_ANNOTATION];
    if (!previousFence) {
      throw new Error(`SandboxClaim ${claimName} is missing ${FENCING_TOKEN_ANNOTATION} for adoption`);
    }
    const deadline = Date.now() + this.config.claimReadyTimeoutMs;
    let fenceTransferred = claim.metadata.annotations?.[FENCING_TOKEN_ANNOTATION] === input.fencingToken;
    try {
      while (!fenceTransferred) {
        throwIfAborted(signal);
        const resourceVersion = claim.metadata.resourceVersion;
        if (!resourceVersion) throw new Error(`SandboxClaim ${claimName} is missing metadata.resourceVersion for adoption`);
        if (claim.metadata.annotations?.[FENCING_TOKEN_ANNOTATION] !== previousFence) {
          throw new Error(`SandboxClaim ${claimName} fencing ownership changed during adoption`);
        }
        try {
          await this.api.patchNamespacedCustomObject({
            group: GROUP,
            version: this.config.sandboxClaimApiVersion,
            namespace: this.config.kubeNamespace,
            plural: PLURAL,
            name: claimName,
            body: {
              metadata: {
                annotations: { [FENCING_TOKEN_ANNOTATION]: input.fencingToken },
                resourceVersion,
              },
            },
          }, setHeaderOptions("Content-Type", PatchStrategy.MergePatch));
          fenceTransferred = true;
        } catch (error) {
          if (!isConflict(error)) throw error;
        }
        claim = await this.getClaim(claimName);
        if (!claim) throw new Error(`SandboxClaim ${claimName} disappeared during adoption`);
        assertOwnership(claim, ownership, new Set([FENCING_TOKEN_ANNOTATION]));
        fenceTransferred = claim.metadata.annotations?.[FENCING_TOKEN_ANNOTATION] === input.fencingToken;
        if (!fenceTransferred) {
          if (Date.now() >= deadline) throw new Error(`Timed out transferring SandboxClaim ${claimName} fencing ownership`);
          await abortableDelay(Math.min(100, Math.max(1, deadline - Date.now())), signal);
        }
      }

      throwIfAborted(signal);
      const adopted = await this.getClaim(claimName);
      if (!adopted) throw new Error(`SandboxClaim ${claimName} disappeared after adoption`);
      assertOwnership(adopted, ownership);
      const password = passwordFromClaim(adopted);
      const log = logger.child({ executionId: input.executionId, attempt: input.attempt, claimName });
      const endpoint = await this.waitForReady(adopted, password, ownership, signal, log);
      return { ...endpoint, release: input };
    } catch (error) {
      const reasonName = signal.reason instanceof Error ? signal.reason.name : undefined;
      if (signal.aborted && reasonName !== "ExecutionCancellationRequestedError" && reasonName !== "TimeoutError") {
        throw error;
      }
      if (!fenceTransferred) throw error;
      try {
        await this.release(input, AbortSignal.timeout(Math.min(this.config.claimReadyTimeoutMs, 10_000)));
      } catch (cleanupError) {
        throw new SandboxClaimCleanupError({ cause: cleanupError });
      }
      throw error;
    }
  }

  async release(input: ExecutionAttemptProvisioningInput, signal: AbortSignal): Promise<void> {
    throwIfAborted(signal);
    const claimName = claimNameForExecutionAttempt(input.executionId, input.attempt);
    const claim = await this.getClaim(claimName);
    if (!claim) return;
    assertOwnership(claim, ownershipAnnotations(input));
    logger.info("releasing sandbox claim", { claimName });
    try {
      await this.api.deleteNamespacedCustomObject({
        group: GROUP,
        version: this.config.sandboxClaimApiVersion,
        namespace: this.config.kubeNamespace,
        plural: PLURAL,
        name: claimName,
        propagationPolicy: "Foreground",
        ...deletePreconditions(claim),
      });
    } catch (error) {
      if (!isNotFound(error)) throw error;
      return;
    }
    try {
      await this.waitForDeleted(claimName, signal);
      sandboxClaims.inc({ tenant: input.tenantId, profile_id: input.profileVersion.profileId, operation: "released", result: "succeeded" });
    } catch (error) {
      sandboxClaims.inc({ tenant: input.tenantId, profile_id: input.profileVersion.profileId, operation: "released", result: "failed" });
      throw error;
    }
  }

  async releaseCancelledExecution(candidate: RequestedCancellationCleanup, signal: AbortSignal): Promise<void> {
    throwIfAborted(signal);
    if (candidate.attempt === null) {
      if (candidate.workloadName !== null) {
        throw new Error(`Cancellation cleanup for execution ${candidate.executionId} has a workload but no attempt`);
      }
      return;
    }
    const expectedName = claimNameForExecutionAttempt(candidate.executionId, candidate.attempt);
    if (candidate.workloadName !== null && candidate.workloadName !== expectedName) {
      throw new Error(`Cancellation cleanup workload ${candidate.workloadName} does not match expected claim ${expectedName}`);
    }
    const deadline = Date.now() + this.config.claimReadyTimeoutMs;
    const quietWindowMs = Math.min(2_000, Math.max(250, Math.floor(this.config.claimReadyTimeoutMs / 5)));
    const pollIntervalMs = Math.min(100, Math.max(25, Math.floor(quietWindowMs / 4)));
    let absentSince: number | undefined;

    while (Date.now() < deadline) {
      throwIfAborted(signal);
      const claim = await this.getClaim(expectedName);
      if (claim) {
        absentSince = undefined;
        assertCancellationOwnership(claim, candidate);
        logger.info("releasing cancelled execution sandbox claim", { claimName: expectedName });
        try {
          await this.api.deleteNamespacedCustomObject({
            group: GROUP,
            version: this.config.sandboxClaimApiVersion,
            namespace: this.config.kubeNamespace,
            plural: PLURAL,
            name: expectedName,
            propagationPolicy: "Foreground",
            ...(claim.metadata.uid ? { body: { preconditions: { uid: claim.metadata.uid } } } : {}),
          });
        } catch (error) {
          if (!isNotFound(error)) throw error;
        }
      } else {
        absentSince ??= Date.now();
        if (Date.now() - absentSince >= quietWindowMs) return;
      }
      await abortableDelay(Math.min(pollIntervalMs, Math.max(1, deadline - Date.now())), signal);
    }
    throw new Error(`Timed out waiting for SandboxClaim ${expectedName} to remain deleted`);
  }

  private buildClaim(
    input: ExecutionAttemptProvisioningInput,
    claimName: string,
    password: string,
    ownership: Record<string, string>,
  ): SandboxClaim {
    const env: SandboxEnvVar[] = [
      { containerName: "opencode", name: "OPENCODE_SERVER_USERNAME", value: "opencode" },
      { containerName: "opencode", name: "OPENCODE_SERVER_PASSWORD", value: password },
      { containerName: "workspace-materializer", name: "DISPATCH_WORKSPACE_TYPE", value: input.workspace.type },
    ];
    const configContent = buildOpencodeConfigContent(input.opencodeConfig);
    if (configContent) env.push({ containerName: "opencode", name: "OPENCODE_CONFIG_CONTENT", value: configContent });
    if (input.workspace.type === "git") {
      env.push(
        { containerName: "workspace-materializer", name: "DISPATCH_WORKSPACE_GIT_URL", value: input.workspace.repository.url },
        { containerName: "workspace-materializer", name: "DISPATCH_WORKSPACE_GIT_COMMIT", value: input.workspace.revision.commit },
      );
      if (input.connections.some(({ sidecar }) => sidecar === "github-token-broker")) {
        env.push(
          { containerName: "github-token-broker", name: "DISPATCH_WORKSPACE_DIRECTORY", value: "/workspace" },
          { containerName: "github-token-broker", name: "DISPATCH_WORKSPACE_GIT_COMMIT", value: input.workspace.revision.commit },
        );
      }
    }
    for (const [sidecar, refs] of groupedConnections(input.connections)) {
      env.push({
        containerName: sidecar,
        name: "DISPATCH_CONNECTIONS",
        value: canonicalJson({ schemaVersion: 1, tenantId: input.tenantId, refs }),
      });
    }
    if (input.controlPlaneUrl && input.connections.some(({ sidecar }) => sidecar === "github-token-broker")) {
      env.push(
        { containerName: "github-token-broker", name: "DISPATCH_EFFECT_ENDPOINT", value: input.controlPlaneUrl },
        { containerName: "github-token-broker", name: "DISPATCH_EXECUTION_ID", value: input.executionId },
        { containerName: "github-token-broker", name: "DISPATCH_EFFECT_TOKEN", value: input.fencingToken },
      );
      if (input.githubMergeCapability) env.push({
        containerName: "github-token-broker",
        name: "DISPATCH_GITHUB_MERGE_CAPABILITY",
        value: canonicalJson({ schemaVersion: 1, ...input.githubMergeCapability }),
      });
    }

    const connectionAnnotations = authorizationAnnotations(input);

    return {
      apiVersion: `extensions.agents.x-k8s.io/${this.config.sandboxClaimApiVersion}`,
      kind: "SandboxClaim",
      metadata: {
        name: claimName,
        namespace: this.config.kubeNamespace,
        labels: {
          "app.kubernetes.io/managed-by": "dispatch",
          "dispatch.dev/execution": labelValue(input.executionId),
          "dispatch.dev/attempt": String(input.attempt),
          "dispatch.dev/profile": labelValue(input.profileVersion.profileId),
        },
        annotations: ownership,
      },
      spec: {
        ...(this.config.sandboxClaimApiVersion === "v1beta1"
          ? { warmPoolRef: { name: input.warmPool! } }
          : {
              sandboxTemplateRef: { name: input.sandboxTemplate },
              ...(input.warmPool === undefined ? {} : { warmpool: input.warmPool }),
            }),
        lifecycle: {
          shutdownTime: input.timeoutAt.toISOString(),
          shutdownPolicy: "DeleteForeground",
          ttlSecondsAfterFinished: input.ttlSecondsAfterFinished,
        },
        env,
        additionalPodMetadata: {
          annotations: {
            ...connectionAnnotations,
            "dispatch.dev/managed-by": "dispatch",
            "dispatch.dev/execution": labelValue(input.executionId),
            "dispatch.dev/attempt": String(input.attempt),
            "dispatch.dev/profile": labelValue(input.profileVersion.profileId),
            "dispatch.dev/claim": claimName,
          },
        },
      },
    };
  }

  private async getClaim(claimName: string): Promise<SandboxClaim | null> {
    try {
      return (await this.api.getNamespacedCustomObject({
        group: GROUP,
        version: this.config.sandboxClaimApiVersion,
        namespace: this.config.kubeNamespace,
        plural: PLURAL,
        name: claimName,
      })) as SandboxClaim;
    } catch (error) {
      if (isNotFound(error)) return null;
      throw error;
    }
  }

  private waitForReady(
    initial: SandboxClaim,
    password: string,
    ownership: Record<string, string>,
    signal: AbortSignal,
    log: ReturnType<typeof logger.child>,
  ): Promise<Omit<ExecutionAttemptEndpoint, "release">> {
    throwIfAborted(signal);
    if (isReady(initial)) return Promise.resolve(this.endpoint(initial, password));

    const claimName = initial.metadata.name;
    return new Promise((resolve, reject) => {
      let settled = false;
      let watchController: AbortController | undefined;
      let reconnectTimer: ReturnType<typeof setTimeout> | undefined;
      const deadline = Date.now() + this.config.claimReadyTimeoutMs;
      const deadlineTimer = setTimeout(
        () => settle(() => reject(new Error(`Timed out waiting for SandboxClaim ${claimName} to become Ready`))),
        this.config.claimReadyTimeoutMs,
      );

      const settle = (finish: () => void): void => {
        if (settled) return;
        settled = true;
        clearTimeout(reconnectTimer);
        clearTimeout(deadlineTimer);
        signal.removeEventListener("abort", onAbort);
        watchController?.abort();
        finish();
      };
      const onAbort = (): void => settle(() => reject(abortError(signal)));
      signal.addEventListener("abort", onAbort, { once: true });

      const onEvent = (phase: string, object: unknown): void => {
        if (phase === "DELETED") {
          settle(() => reject(new Error(`SandboxClaim ${claimName} was deleted before becoming Ready`)));
          return;
        }
        if (phase !== "ADDED" && phase !== "MODIFIED") return;
        const claim = object as SandboxClaim;
        try {
          assertOwnership(claim, ownership);
        } catch (error) {
          settle(() => reject(asError(error)));
          return;
        }
        const rejected = rejectedCondition(claim);
        if (rejected) {
          settle(() => reject(new SandboxClaimRejectedError(claimName, rejected)));
          return;
        }
        if (!isReady(claim)) return;
        try {
          const endpoint = this.endpoint(claim, password);
          log.info("sandbox claim ready", { host: endpoint.host });
          settle(() => resolve(endpoint));
        } catch (error) {
          settle(() => reject(asError(error)));
        }
      };
      const onDone = (error: unknown): void => {
        if (settled) return;
        if (error) log.debug("watch ended with error, will reconnect", { claim: claimName });
        const remaining = deadline - Date.now();
        if (remaining > 0) reconnectTimer = setTimeout(() => void startWatch(), Math.min(1_000, remaining));
      };
      const startWatch = async (): Promise<void> => {
        if (settled) return;
        try {
          watchController = await this.watch.watch(
            this.claimsWatchPath(),
            { fieldSelector: `metadata.name=${claimName}` },
            onEvent,
            onDone,
          );
          if (settled) watchController.abort();
          else {
            const current = await this.getClaim(claimName);
            if (!current) onEvent("DELETED", initial);
            else onEvent("MODIFIED", current);
          }
        } catch (error) {
          onDone(error);
        }
      };
      void startWatch();
    });
  }

  private waitForDeleted(claimName: string, signal: AbortSignal): Promise<void> {
    throwIfAborted(signal);
    return new Promise((resolve, reject) => {
      let settled = false;
      let watchController: AbortController | undefined;
      let reconnectTimer: ReturnType<typeof setTimeout> | undefined;
      const deadline = Date.now() + this.config.claimReadyTimeoutMs;
      const deadlineTimer = setTimeout(
        () => settle(() => reject(new Error(`Timed out waiting for SandboxClaim ${claimName} to be deleted`))),
        this.config.claimReadyTimeoutMs,
      );

      const settle = (finish: () => void): void => {
        if (settled) return;
        settled = true;
        clearTimeout(reconnectTimer);
        clearTimeout(deadlineTimer);
        signal.removeEventListener("abort", onAbort);
        watchController?.abort();
        finish();
      };
      const onAbort = (): void => settle(() => reject(abortError(signal)));
      signal.addEventListener("abort", onAbort, { once: true });
      const onEvent = (phase: string): void => {
        if (phase === "DELETED") {
          void this.getClaim(claimName).then((claim) => {
            if (!claim) settle(resolve);
          }, reconnect);
        }
      };
      const reconnect = (): void => {
        const remaining = deadline - Date.now();
        if (remaining > 0) reconnectTimer = setTimeout(() => void startWatch(), Math.min(1_000, remaining));
      };
      const onDone = (error: unknown): void => {
        if (settled) return;
        if (error) logger.debug("watch ended with error while waiting for deletion", { claim: claimName });
        void this.getClaim(claimName).then((claim) => claim ? reconnect() : settle(resolve), reconnect);
      };
      const startWatch = async (): Promise<void> => {
        if (settled) return;
        try {
          watchController = await this.watch.watch(
            this.claimsWatchPath(),
            { fieldSelector: `metadata.name=${claimName}` },
            onEvent,
            onDone,
          );
          if (settled) watchController.abort();
          else if (!await this.getClaim(claimName)) settle(resolve);
        } catch (error) {
          onDone(error);
        }
      };
      void this.getClaim(claimName).then((claim) => claim ? void startWatch() : settle(resolve), () => void startWatch());
    });
  }

  private endpoint(claim: SandboxClaim, password: string): Omit<ExecutionAttemptEndpoint, "release"> {
    const host = claim.status?.sandbox?.podIPs?.[0]
      ?? (claim.status?.sandbox?.name ? `${claim.status.sandbox.name}.${this.config.kubeNamespace}.svc` : undefined);
    if (!host) throw new Error(`Ready SandboxClaim ${claim.metadata.name} did not expose a service FQDN, sandbox name, or pod IP`);
    return { workloadName: claim.metadata.name, host, password };
  }

  private claimsWatchPath(): string {
    return `/apis/${GROUP}/${this.config.sandboxClaimApiVersion}/namespaces/${this.config.kubeNamespace}/${PLURAL}`;
  }
}

function ownershipAnnotations(input: ExecutionAttemptProvisioningInput): Record<string, string> {
  return {
    [FENCING_TOKEN_ANNOTATION]: input.fencingToken,
    "dispatch.dev/tenant-id": input.tenantId,
    "dispatch.dev/execution-id": input.executionId,
    "dispatch.dev/attempt": String(input.attempt),
    "dispatch.dev/profile-version-id": input.profileVersion.id,
    "dispatch.dev/profile-id": input.profileVersion.profileId,
    "dispatch.dev/profile-version": String(input.profileVersion.version),
    "dispatch.dev/workspace-digest": createHash("sha256").update(canonicalJson(input.workspace)).digest("hex"),
    [CONNECTIONS_DIGEST_ANNOTATION]: authorizationAnnotations(input)[CONNECTIONS_DIGEST_ANNOTATION]!,
  };
}

function authorizationAnnotations(input: ExecutionAttemptProvisioningInput): Record<string, string> {
  const authorization = canonicalJson(sortedConnections(input.connections));
  return {
    [CONNECTIONS_DIGEST_ANNOTATION]: createHash("sha256").update(authorization).digest("hex"),
  };
}

function sortedConnections(connections: ExecutionAttemptProvisioningInput["connections"]): Array<{ id: string; sidecar: string }> {
  return connections
    .map(({ id, sidecar }) => ({ id, sidecar }))
    .sort((left, right) => compareStrings(left.id, right.id) || compareStrings(left.sidecar, right.sidecar));
}

function compareStrings(left: string, right: string): number {
  return left < right ? -1 : left > right ? 1 : 0;
}

function groupedConnections(connections: ExecutionAttemptProvisioningInput["connections"]): Array<[string, string[]]> {
  const grouped = new Map<string, string[]>();
  for (const { id, sidecar } of connections) {
    const refs = grouped.get(sidecar);
    if (refs) refs.push(id);
    else grouped.set(sidecar, [id]);
  }
  return [...grouped]
    .sort(([left], [right]) => compareStrings(left, right))
    .map(([sidecar, refs]) => [sidecar, refs.sort(compareStrings)]);
}

function canonicalJson(value: unknown): string {
  if (Array.isArray(value)) return `[${value.map(canonicalJson).join(",")}]`;
  if (value !== null && typeof value === "object") {
    return `{${Object.entries(value as Record<string, unknown>)
      .sort(([left], [right]) => left < right ? -1 : left > right ? 1 : 0)
      .map(([key, entry]) => `${JSON.stringify(key)}:${canonicalJson(entry)}`)
      .join(",")}}`;
  }
  return JSON.stringify(value) ?? "null";
}

function assertOwnership(claim: SandboxClaim, expected: Record<string, string>, ignored = new Set<string>()): void {
  const actual = claim.metadata.annotations ?? {};
  const mismatch = Object.entries(expected).find(([key, value]) => !ignored.has(key) && actual[key] !== value);
  if (mismatch) {
    throw new Error(`Existing SandboxClaim ${claim.metadata.name} is not owned by this execution attempt: annotation ${mismatch[0]} does not match`);
  }
}

function deletePreconditions(claim: SandboxClaim): { body: { preconditions: { uid?: string; resourceVersion?: string } } } | object {
  const preconditions = claim.metadata.uid
    ? { uid: claim.metadata.uid }
    : claim.metadata.resourceVersion
      ? { resourceVersion: claim.metadata.resourceVersion }
      : {};
  return Object.keys(preconditions).length > 0 ? { body: { preconditions } } : {};
}

function assertCancellationOwnership(claim: SandboxClaim, candidate: RequestedCancellationCleanup): void {
  const labels = claim.metadata.labels ?? {};
  const annotations = claim.metadata.annotations ?? {};
  const expected = {
    "app.kubernetes.io/managed-by": "dispatch",
    "dispatch.dev/tenant-id": candidate.tenantId,
    "dispatch.dev/execution-id": candidate.executionId,
    "dispatch.dev/attempt": String(candidate.attempt),
  };
  const actual: Record<string, string | undefined> = {
    ...annotations,
    "app.kubernetes.io/managed-by": labels["app.kubernetes.io/managed-by"],
  };
  const mismatch = Object.entries(expected).find(([key, value]) => actual[key] !== value);
  if (mismatch) {
    throw new Error(`SandboxClaim ${claim.metadata.name} is not owned by this cancellation candidate: ${mismatch[0]} does not match`);
  }
}

function passwordFromClaim(claim: SandboxClaim): string {
  const password = claim.spec?.env?.find((entry) => entry.name === "OPENCODE_SERVER_PASSWORD")?.value;
  if (!password) throw new Error(`Existing SandboxClaim ${claim.metadata.name} is missing OPENCODE_SERVER_PASSWORD`);
  return password;
}

function isReady(claim: SandboxClaim): boolean {
  return claim.status?.conditions?.some((condition) => condition.type === "Ready" && condition.status === "True") ?? false;
}

function rejectedCondition(claim: SandboxClaim): string | undefined {
  const condition = claim.status?.conditions?.find(({ type, status, reason }) =>
    type === "Ready" && status === "False" && reason !== undefined && TERMINAL_CLAIM_REASONS.has(reason));
  if (!condition) return undefined;
  return [condition.reason, condition.message].filter(Boolean).join(": ") || "controller reported Ready=False";
}

function isNotFound(error: unknown): boolean {
  return statusCode(error) === 404;
}

function isConflict(error: unknown): boolean {
  return statusCode(error) === 409;
}

function statusCode(error: unknown): number | undefined {
  if (!error || typeof error !== "object") return undefined;
  const candidate = error as { code?: number; response?: { statusCode?: number; status?: number } };
  return candidate.code ?? candidate.response?.statusCode ?? candidate.response?.status;
}

function labelValue(value: string): string {
  if (value.length <= 63 && /^(([A-Za-z0-9][-A-Za-z0-9_.]*)?[A-Za-z0-9])?$/.test(value)) return value;
  return `h-${createHash("sha256").update(value).digest("hex").slice(0, 61)}`;
}

function throwIfAborted(signal: AbortSignal): void {
  if (signal.aborted) throw abortError(signal);
}

function abortError(signal: AbortSignal): Error {
  return signal.reason instanceof Error ? signal.reason : new DOMException("The operation was aborted", "AbortError");
}

function abortableDelay(delayMs: number, signal: AbortSignal): Promise<void> {
  throwIfAborted(signal);
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => {
      signal.removeEventListener("abort", onAbort);
      resolve();
    }, delayMs);
    const onAbort = (): void => {
      clearTimeout(timer);
      signal.removeEventListener("abort", onAbort);
      reject(abortError(signal));
    };
    signal.addEventListener("abort", onAbort, { once: true });
  });
}

function asError(error: unknown): Error {
  return error instanceof Error ? error : new Error(String(error));
}
