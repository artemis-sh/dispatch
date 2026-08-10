import { z } from "zod";
import { normalizedCloudEventSchema, type NormalizedCloudEvent } from "../../execution/events.js";

const BODY_LIMIT_BYTES = 16 * 1024;
const supportedActions: Record<GitHubEventName, ReadonlySet<string>> = {
  issues: new Set(["opened", "edited", "closed", "reopened", "assigned", "unassigned", "labeled", "unlabeled"]),
  issue_comment: new Set(["created", "edited", "deleted"]),
  pull_request: new Set(["opened", "edited", "closed", "reopened", "synchronize", "ready_for_review", "converted_to_draft", "review_requested", "review_request_removed", "assigned", "unassigned", "labeled", "unlabeled"]),
  pull_request_review: new Set(["submitted", "edited", "dismissed"]),
  pull_request_review_comment: new Set(["created", "edited", "deleted"]),
  workflow_run: new Set(["completed"]),
} as const;

const bounded = (limit: number) => z.string().min(1).refine((value) => Buffer.byteLength(value, "utf8") <= limit);
const id = z.number().int().safe().nonnegative();
const timestamp = z.iso.datetime({ offset: true }).transform((value) => new Date(value).toISOString());
const nullableTimestamp = timestamp.nullable();
const fullName = bounded(255).regex(/^[A-Za-z0-9](?:[A-Za-z0-9-]{0,38})\/[A-Za-z0-9_.-]+$/);
const sha = z.string().regex(/^[0-9a-fA-F]{40}$/).transform((value) => value.toLowerCase());

const actorSchema = z.object({ id, login: bounded(255), type: bounded(64) });
const labelSchema = z.object({ name: bounded(255) });
const repositorySchema = z.object({ id, full_name: fullName, clone_url: bounded(2_048), default_branch: bounded(255), private: z.boolean() });
const gitRepositorySchema = z.object({ id, full_name: fullName, clone_url: bounded(2_048) });
const workflowRunHeadRepositorySchema = z.object({ id, full_name: fullName });

const issueSchema = z.object({
  number: z.number().int().positive(), title: bounded(4_096), body: z.string().nullable(), state: z.enum(["open", "closed"]), user: actorSchema,
  labels: z.array(labelSchema).max(1_000), assignees: z.array(actorSchema).max(1_000), created_at: timestamp, updated_at: timestamp, closed_at: nullableTimestamp,
});
const branchShape = { sha, ref: bounded(255) };
const headBranchSchema = z.object({ ...branchShape, repo: gitRepositorySchema.nullable() });
const baseBranchSchema = z.object({ ...branchShape, repo: gitRepositorySchema });
const pullRequestSchema = z.object({
  id, number: z.number().int().positive(), title: bounded(4_096), body: z.string().nullable(), draft: z.boolean(), state: z.enum(["open", "closed"]), merged: z.boolean(), user: actorSchema,
  head: headBranchSchema, base: baseBranchSchema, labels: z.array(labelSchema).max(1_000), assignees: z.array(actorSchema).max(1_000), requested_reviewers: z.array(actorSchema).max(1_000),
  created_at: timestamp, updated_at: timestamp, closed_at: nullableTimestamp, merged_at: nullableTimestamp,
});
const commentSchema = z.object({ id, body: z.string(), user: actorSchema, created_at: timestamp, updated_at: timestamp });
const reviewSchema = z.object({ id, body: z.string().nullable(), user: actorSchema, state: bounded(64), commit_id: sha.nullable(), submitted_at: nullableTimestamp });
const reviewCommentSchema = commentSchema.extend({ path: bounded(4_096), line: z.number().int().positive().nullable(), original_line: z.number().int().positive().nullable(), side: z.enum(["LEFT", "RIGHT"]).nullable(), commit_id: sha, in_reply_to_id: id.optional() });
const commonPayloadShape = { action: bounded(64), installation: z.object({ id }).nullish(), repository: repositorySchema, sender: actorSchema };
const issuesPayloadSchema = z.object({ ...commonPayloadShape, issue: issueSchema });
const pullRequestPayloadSchema = z.object({ ...commonPayloadShape, pull_request: pullRequestSchema });
const issueCommentPayloadSchema = z.object({ ...commonPayloadShape, issue: issueSchema, comment: commentSchema });
const pullRequestReviewPayloadSchema = z.object({ ...commonPayloadShape, pull_request: pullRequestSchema.extend({ merged: z.boolean().default(false) }), review: reviewSchema });
const pullRequestReviewCommentPayloadSchema = z.object({ ...commonPayloadShape, pull_request: pullRequestSchema, comment: reviewCommentSchema });
const workflowRunPayloadSchema = z.object({
  ...commonPayloadShape,
  workflow_run: z.object({
    id, name: bounded(255), event: bounded(64), status: z.literal("completed"), conclusion: z.enum(["action_required", "cancelled", "failure", "neutral", "skipped", "stale", "startup_failure", "success", "timed_out"]),
    head_sha: sha, head_branch: bounded(255).nullable(), head_repository: workflowRunHeadRepositorySchema.nullable(),
    pull_requests: z.array(z.object({ id, number: z.number().int().positive(), head: z.object({ ref: bounded(255), sha }), base: z.object({ ref: bounded(255), sha }) })).max(100),
  }),
});
const envelopeSchema = z.object({ action: bounded(64) });

function dispatchReviewVerdict(body: string | null): "approved" | "changes_requested" | undefined {
  const match = body?.match(/^Dispatch-Verdict:[ \t]*(approved|changes_requested)[ \t]*(?:\r?\n|$)/i);
  return match?.[1]?.toLowerCase() as "approved" | "changes_requested" | undefined;
}
export type GitHubEventName = "issues" | "issue_comment" | "pull_request" | "pull_request_review" | "pull_request_review_comment" | "workflow_run";
export type NormalizeGitHubEventInput = { event: GitHubEventName; deliveryId: string; payloadSha256: string; payload: unknown };
function cloneUrl(repository: { full_name: string; clone_url: string }): string {
  const canonical = `https://github.com/${repository.full_name}.git`;
  let supplied: URL;
  try { supplied = new URL(repository.clone_url); } catch { throw new Error("GitHub repository clone_url must be a valid URL"); }
  if (supplied.protocol !== "https:" || supplied.hostname.toLowerCase() !== "github.com" || supplied.username !== "" || supplied.password !== "" || supplied.search !== "" || supplied.hash !== "" || supplied.pathname.toLowerCase() !== `/${repository.full_name}.git`.toLowerCase()) throw new Error("GitHub repository clone_url must match its full_name on github.com");
  return canonical;
}
function truncateBody(body: string | null): { body: string | null; bodyTruncated: boolean } {
  if (body === null) return { body: null, bodyTruncated: false };
  const bytes = Buffer.from(body, "utf8");
  if (bytes.length <= BODY_LIMIT_BYTES) return { body, bodyTruncated: false };
  let end = BODY_LIMIT_BYTES;
  while (end > 0 && (bytes[end]! & 0xc0) === 0x80) end -= 1;
  return { body: bytes.subarray(0, end).toString("utf8"), bodyTruncated: true };
}
function actor(value: z.infer<typeof actorSchema>) { return { id: value.id, login: value.login, type: value.type }; }
function actors(values: z.infer<typeof actorSchema>[]) { return values.map(actor).sort((left, right) => compareStrings(left.login, right.login) || left.id - right.id); }
function labels(values: z.infer<typeof labelSchema>[]) { return values.map((label) => label.name).sort(compareStrings); }
function compareStrings(left: string, right: string): number { return left < right ? -1 : left > right ? 1 : 0; }
function gitBranch(value: z.infer<typeof headBranchSchema>) { return { sha: value.sha, ref: value.ref, repository: value.repo === null ? null : { id: value.repo.id, fullName: value.repo.full_name, cloneUrl: cloneUrl(value.repo) } }; }
function normalizedIssue(issue: z.infer<typeof issueSchema>) { return { number: issue.number, title: issue.title, ...truncateBody(issue.body), state: issue.state, user: actor(issue.user), labels: labels(issue.labels), assignees: actors(issue.assignees), createdAt: issue.created_at, updatedAt: issue.updated_at, closedAt: issue.closed_at }; }
function normalizedPullRequest(pullRequest: z.infer<typeof pullRequestSchema>) { return { id: pullRequest.id, number: pullRequest.number, title: pullRequest.title, ...truncateBody(pullRequest.body), draft: pullRequest.draft, state: pullRequest.state, merged: pullRequest.merged, user: actor(pullRequest.user), head: gitBranch(pullRequest.head), base: gitBranch(pullRequest.base), labels: labels(pullRequest.labels), assignees: actors(pullRequest.assignees), requestedReviewers: actors(pullRequest.requested_reviewers), createdAt: pullRequest.created_at, updatedAt: pullRequest.updated_at, closedAt: pullRequest.closed_at, mergedAt: pullRequest.merged_at }; }

export function normalizeGitHubEvent(input: NormalizeGitHubEventInput): NormalizedCloudEvent | null {
  const event = z.enum(["issues", "issue_comment", "pull_request", "pull_request_review", "pull_request_review_comment", "workflow_run"]).parse(input.event);
  const action = envelopeSchema.parse(input.payload).action;
  if (!supportedActions[event].has(action)) return null;
  const payload = event === "issues" ? issuesPayloadSchema.parse(input.payload) : event === "issue_comment" ? issueCommentPayloadSchema.parse(input.payload) : event === "pull_request" ? pullRequestPayloadSchema.parse(input.payload) : event === "pull_request_review" ? pullRequestReviewPayloadSchema.parse(input.payload) : event === "pull_request_review_comment" ? pullRequestReviewCommentPayloadSchema.parse(input.payload) : workflowRunPayloadSchema.parse(input.payload);
  if (payload.action !== action) throw new Error("GitHub payload action changed during normalization");
  const repository = { id: payload.repository.id, fullName: payload.repository.full_name, cloneUrl: cloneUrl(payload.repository), defaultBranch: payload.repository.default_branch, private: payload.repository.private };
  const common = { schemaVersion: 1, deliveryId: z.string().min(1).max(1_024).parse(input.deliveryId), action, installationId: payload.installation?.id ?? null, repository, sender: actor(payload.sender) };
  let subject: string | undefined;
  const data = event === "issues" ? (() => { const issue = (payload as z.infer<typeof issuesPayloadSchema>).issue; subject = `issues/${issue.number}`; return { ...common, issue: normalizedIssue(issue) }; })() : event === "issue_comment" ? (() => { const value = payload as z.infer<typeof issueCommentPayloadSchema>; subject = `issues/${value.issue.number}`; return { ...common, issue: normalizedIssue(value.issue), comment: { id: value.comment.id, ...truncateBody(value.comment.body), user: actor(value.comment.user), createdAt: value.comment.created_at, updatedAt: value.comment.updated_at } }; })() : event === "pull_request" ? (() => { const pullRequest = (payload as z.infer<typeof pullRequestPayloadSchema>).pull_request; subject = `pulls/${pullRequest.number}`; return { ...common, pullRequest: normalizedPullRequest(pullRequest) }; })() : event === "pull_request_review" ? (() => { const value = payload as z.infer<typeof pullRequestReviewPayloadSchema>; const dispatchVerdict = dispatchReviewVerdict(value.review.body); subject = `pulls/${value.pull_request.number}`; return { ...common, pullRequest: normalizedPullRequest(value.pull_request), review: { id: value.review.id, ...truncateBody(value.review.body), ...(dispatchVerdict ? { dispatchVerdict } : {}), user: actor(value.review.user), state: value.review.state.toLowerCase(), commitSha: value.review.commit_id, submittedAt: value.review.submitted_at } }; })() : event === "pull_request_review_comment" ? (() => { const value = payload as z.infer<typeof pullRequestReviewCommentPayloadSchema>; subject = `pulls/${value.pull_request.number}`; return { ...common, pullRequest: normalizedPullRequest(value.pull_request), comment: { id: value.comment.id, ...truncateBody(value.comment.body), user: actor(value.comment.user), path: value.comment.path, line: value.comment.line, originalLine: value.comment.original_line, side: value.comment.side, commitSha: value.comment.commit_id, inReplyToId: value.comment.in_reply_to_id ?? null, createdAt: value.comment.created_at, updatedAt: value.comment.updated_at } }; })() : (() => {
    const value = payload as z.infer<typeof workflowRunPayloadSchema>;
    const run = value.workflow_run;
    if (run.pull_requests.length !== 1 || run.head_repository === null) return null;
    const pullRequest = run.pull_requests[0]!;
    if (pullRequest.head.sha !== run.head_sha) return null;
    subject = `pulls/${pullRequest.number}`;
    return { ...common, pullRequest: { id: pullRequest.id, number: pullRequest.number, head: { ref: pullRequest.head.ref, sha: pullRequest.head.sha, repository: { id: run.head_repository.id, fullName: run.head_repository.full_name, cloneUrl: `https://github.com/${run.head_repository.full_name}.git` } }, base: { ref: pullRequest.base.ref, sha: pullRequest.base.sha } }, workflowRun: { id: run.id, name: run.name, event: run.event, status: run.status, conclusion: run.conclusion, headSha: run.head_sha, headBranch: run.head_branch } };
  })();
  if (data === null) return null;
  if (!subject) throw new Error("GitHub event subject was not projected");
  return normalizedCloudEventSchema.parse({ specversion: "1.0", id: common.deliveryId, source: `https://github.com/${repository.fullName}`, type: `com.github.${event}.${action}`, subject, datacontenttype: "application/json", githubevent: event, githubpayloadsha256: z.string().regex(/^[0-9a-f]{64}$/).parse(input.payloadSha256), data });
}
