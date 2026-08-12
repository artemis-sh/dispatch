CREATE TABLE "dispatch_event_revision_resolution_candidates" (
	"binding_version_id" text NOT NULL,
	"event_id" text NOT NULL,
	"tenant_id" text NOT NULL,
	CONSTRAINT "dispatch_event_revision_resolution_candidates_pk" PRIMARY KEY("event_id","tenant_id","binding_version_id")
);
--> statement-breakpoint
ALTER TABLE "dispatch_event_revision_resolution_candidates" ADD CONSTRAINT "dispatch_event_revision_resolution_candidates_resolution_fk" FOREIGN KEY ("event_id","tenant_id") REFERENCES "public"."dispatch_event_revision_resolutions"("event_id","tenant_id") ON DELETE cascade ON UPDATE no action;
--> statement-breakpoint
ALTER TABLE "dispatch_event_revision_resolution_candidates" ADD CONSTRAINT "dispatch_event_revision_resolution_candidates_binding_fk" FOREIGN KEY ("binding_version_id","tenant_id") REFERENCES "public"."dispatch_binding_versions"("id","tenant_id") ON DELETE no action ON UPDATE no action;
--> statement-breakpoint
INSERT INTO "dispatch_event_revision_resolution_candidates" ("event_id", "tenant_id", "binding_version_id")
SELECT resolution.event_id, resolution.tenant_id, binding.id
FROM dispatch_event_revision_resolutions AS resolution
JOIN dispatch_events AS event ON event.id = resolution.event_id AND event.tenant_id = resolution.tenant_id
JOIN dispatch_binding_versions AS binding ON binding.tenant_id = event.tenant_id AND binding.trigger_id = event.trigger_id
  AND binding.enabled AND event.type = ANY(binding.event_types)
WHERE resolution.state IN ('PENDING', 'LEASED', 'RETRY_WAIT');
