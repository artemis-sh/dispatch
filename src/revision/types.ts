import type { AdmissionCommand, AdmissionResult } from "../control/admission.js";

export type GitHubRevisionResolutionRequest = {
  provider: "github";
  installationId: number;
  repositoryId: number;
  repositoryFullName: string;
  cloneUrl: string;
  branch: string;
};

export type ClaimedRevisionResolution = GitHubRevisionResolutionRequest & {
  eventId: string;
  tenantId: string;
  leaseOwner: string;
  leaseToken: string;
  attempt: number;
};

export interface RevisionResolutionStore {
  claimRevisionResolution(input: { leaseOwner: string; leaseDurationMs: number }): Promise<ClaimedRevisionResolution | undefined>;
  completeRevisionResolution(input: {
    eventId: string;
    tenantId: string;
    leaseOwner: string;
    leaseToken: string;
    commit: string;
    resolvedAt: string;
  }): Promise<AdmissionResult | undefined>;
  failRevisionResolution(input: {
    eventId: string;
    tenantId: string;
    leaseOwner: string;
    leaseToken: string;
    error: string;
    failedAt: string;
    retryAt: string;
    maxAttempts: number;
  }): Promise<boolean>;
}

export type RevisionAwareAdmissionCommand = AdmissionCommand & {
  revisionResolution?: GitHubRevisionResolutionRequest;
  /** Whether this process has a worker that can resolve deferred revisions. */
  revisionResolverEnabled?: boolean;
  githubIssueAcknowledgment?: {
    subjectType: "issue" | "pull_request";
    installationId: number;
    repositoryId: number;
    repositoryFullName: string;
    issueNumber: number;
  };
};
