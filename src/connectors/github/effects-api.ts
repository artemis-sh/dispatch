import { z } from "zod";
import type { OpenAPIHono } from "@hono/zod-openapi";
import { bodyLimit } from "hono/body-limit";
import { hashCanonicalJson } from "../../json.js";

const registerSchema = z.object({
  executionId: z.string().min(1).max(255), repositoryId: z.number().int().positive(), repositoryFullName: z.string().min(3).max(255),
  request: z.object({ owner: z.string().min(1).max(100), repo: z.string().min(1).max(100), title: z.string().min(1).max(4096), head: z.string().min(1).max(255), base: z.string().min(1).max(255), body: z.string().max(65536).optional(), draft: z.boolean().optional(), maintainer_can_modify: z.boolean().optional(), reviewers: z.array(z.string().min(1).max(100)).max(100).optional() }).strict(),
}).strict();
const reportSchema = z.object({
  executionId: z.string().min(1), githubPullRequestId: z.string().regex(/^[1-9][0-9]*$/), pullRequestNumber: z.number().int().positive(), pullRequestUrl: z.url(),
}).strict();

export type GitHubEffectStore = {
  registerGitHubPullRequestEffect(command: { baseRef: string; executionId: string; fencingToken: string; headRef: string; pullRequestTitle: string; registeredAt: string; repositoryFullName: string; repositoryId: number; requestHash: string; tenantId: string }): Promise<{ created: boolean; id: string; state: string }>;
  reportGitHubPullRequestEffect(command: { effectId: string; executionId: string; fencingToken: string; githubPullRequestId: string; pullRequestNumber: number; pullRequestUrl: string; reportedAt: string; tenantId: string }): Promise<{ id: string; state: string }>;
  listGitHubIssueLifecycles(command: { executionId: string; fencingToken: string; repositoryId: number; tenantId: string }): Promise<unknown[]>;
};

export function mountGitHubEffectsApi(app: OpenAPIHono<any>, store: GitHubEffectStore): void {
  app.use("/internal/v1/github/*", bodyLimit({ maxSize: 64 * 1024, onError: (context) => context.json({ error: "Request body too large" }, 413) }));
  app.post("/internal/v1/github/pull-request-effects", async (context) => {
    const token = bearer(context.req.header("authorization"));
    if (!token) return context.json({ error: "Unauthorized" }, 401);
    const parsed = registerSchema.safeParse(await context.req.json().catch(() => undefined));
    if (!parsed.success) return context.json({ error: "Invalid request" }, 400);
    try {
      const result = await store.registerGitHubPullRequestEffect({ ...parsed.data, tenantId: "default", fencingToken: token,
        baseRef: parsed.data.request.base, headRef: parsed.data.request.head, pullRequestTitle: parsed.data.request.title,
        requestHash: hashCanonicalJson(JSON.parse(JSON.stringify(parsed.data.request))), registeredAt: new Date().toISOString() });
      return context.json(result, 200);
    } catch { return context.json({ error: "Effect registration rejected" }, 409); }
  });
  app.post("/internal/v1/github/pull-request-effects/:effectId/report", async (context) => {
    const token = bearer(context.req.header("authorization"));
    if (!token) return context.json({ error: "Unauthorized" }, 401);
    const parsed = reportSchema.safeParse(await context.req.json().catch(() => undefined));
    if (!parsed.success) return context.json({ error: "Invalid request" }, 400);
    try {
      const result = await store.reportGitHubPullRequestEffect({ ...parsed.data, tenantId: "default", effectId: context.req.param("effectId"), fencingToken: token, reportedAt: new Date().toISOString() });
      return context.json(result, 200);
    } catch { return context.json({ error: "Effect report rejected" }, 409); }
  });
  app.post("/internal/v1/github/issue-lifecycles", async (context) => {
    const token = bearer(context.req.header("authorization"));
    if (!token) return context.json({ error: "Unauthorized" }, 401);
    const parsed = z.object({ executionId: z.string().min(1).max(255), repositoryId: z.number().int().positive() }).strict()
      .safeParse(await context.req.json().catch(() => undefined));
    if (!parsed.success) return context.json({ error: "Invalid request" }, 400);
    try {
      return context.json(await store.listGitHubIssueLifecycles({ ...parsed.data, tenantId: "default", fencingToken: token }), 200);
    } catch { return context.json({ error: "Lifecycle query rejected" }, 409); }
  });
}

function bearer(value: string | undefined): string | undefined {
  return value?.startsWith("Bearer ") && value.length > 7 ? value.slice(7) : undefined;
}
