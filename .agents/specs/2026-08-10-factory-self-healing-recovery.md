---
title: "Factory Self-Healing Recovery Checklist"
kind: notes
created: 2026-08-10T07:51:20+00:00
---

# Factory Self-Healing Recovery Checklist

Status: complete

- [x] Maintenance reads durable Dispatch lifecycle state before recovering work.
- [x] Missing PR effects have a bounded retry budget and escalation outcome.
- [x] Maintenance can apply/reapply PR review routing through the broker.
- [x] Dispatch service account can patch SandboxClaims and recovery is verified.
- [x] Existing green PR backlog is requeued and reaches review/merge or a legitimate change request.
- [x] Final unattended validation shows no blind duplicate recovery or infrastructure-caused stalls.
