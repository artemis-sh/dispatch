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
CREATE FUNCTION "dispatch_migration_0018_resolve_json_pointer"("document" jsonb, "pointer" text)
RETURNS TABLE("found" boolean, "value" jsonb)
LANGUAGE plpgsql
IMMUTABLE
AS $$
DECLARE
	"current_value" jsonb := "document";
	"token" text;
BEGIN
	IF "pointer" = '' THEN
		RETURN QUERY SELECT true, "current_value";
		RETURN;
	END IF;

	FOREACH "token" IN ARRAY regexp_split_to_array(substring("pointer" FROM 2), '/') LOOP
		"token" := replace(replace("token", '~1', '/'), '~0', '~');
		IF jsonb_typeof("current_value") = 'object' THEN
			IF NOT "current_value" ? "token" THEN
				RETURN QUERY SELECT false, NULL::jsonb;
				RETURN;
			END IF;
			"current_value" := "current_value" -> "token";
		ELSIF jsonb_typeof("current_value") = 'array' THEN
			IF "token" !~ '^(0|[1-9][0-9]*)$' OR length("token") > 10 THEN
				RETURN QUERY SELECT false, NULL::jsonb;
				RETURN;
			END IF;
			IF "token"::numeric > 2147483647 THEN
				RETURN QUERY SELECT false, NULL::jsonb;
				RETURN;
			END IF;
			IF "token"::integer >= jsonb_array_length("current_value") THEN
				RETURN QUERY SELECT false, NULL::jsonb;
				RETURN;
			END IF;
			"current_value" := "current_value" -> "token"::integer;
		ELSE
			RETURN QUERY SELECT false, NULL::jsonb;
			RETURN;
		END IF;
	END LOOP;

	RETURN QUERY SELECT true, "current_value";
END;
$$;
--> statement-breakpoint
INSERT INTO "dispatch_event_revision_resolution_candidates" ("event_id", "tenant_id", "binding_version_id")
SELECT resolution.event_id, resolution.tenant_id, binding.id
FROM dispatch_event_revision_resolutions AS resolution
JOIN dispatch_events AS event ON event.id = resolution.event_id AND event.tenant_id = resolution.tenant_id
JOIN dispatch_binding_versions AS binding ON binding.tenant_id = event.tenant_id AND binding.trigger_id = event.trigger_id
  AND binding.created_at <= event.ingested_at
  AND (binding.disabled_at IS NULL OR binding.disabled_at > event.ingested_at)
  AND event.type = ANY(binding.event_types)
  AND NOT (binding.definition ? 'disposition')
  AND binding.definition->'workspace'->>'type' = 'git'
  AND binding.definition->'workspace'->'revision'->'commit'->>'path' = '/repository/defaultBranchRevision/commit'
WHERE resolution.state IN ('PENDING', 'LEASED', 'RETRY_WAIT')
  AND NOT EXISTS (
	SELECT 1
	FROM jsonb_array_elements(binding.definition->'filter'->'all') AS filter_clause
	CROSS JOIN LATERAL dispatch_migration_0018_resolve_json_pointer(event.data, filter_clause->>'path') AS resolved
	WHERE NOT CASE filter_clause->>'op'
		WHEN 'exists' THEN resolved.found = (filter_clause->>'value')::boolean
		WHEN 'eq' THEN resolved.found
			AND jsonb_typeof(resolved.value) NOT IN ('array', 'object')
			AND resolved.value = filter_clause->'value'
		WHEN 'in' THEN resolved.found
			AND jsonb_typeof(resolved.value) NOT IN ('array', 'object')
			AND EXISTS (SELECT 1 FROM jsonb_array_elements(filter_clause->'values') AS expected(value) WHERE expected.value = resolved.value)
		WHEN 'contains' THEN resolved.found
			AND jsonb_typeof(resolved.value) = 'array'
			AND NOT EXISTS (SELECT 1 FROM jsonb_array_elements(resolved.value) AS item(value) WHERE jsonb_typeof(item.value) IN ('array', 'object'))
			AND EXISTS (SELECT 1 FROM jsonb_array_elements(resolved.value) AS item(value) WHERE item.value = filter_clause->'value')
		WHEN 'containsAny' THEN resolved.found
			AND jsonb_typeof(resolved.value) = 'array'
			AND NOT EXISTS (SELECT 1 FROM jsonb_array_elements(resolved.value) AS item(value) WHERE jsonb_typeof(item.value) IN ('array', 'object'))
			AND EXISTS (
				SELECT 1 FROM jsonb_array_elements(resolved.value) AS item(value)
				JOIN jsonb_array_elements(filter_clause->'values') AS expected(value) ON expected.value = item.value
			)
		WHEN 'notStartsWith' THEN resolved.found
			AND jsonb_typeof(resolved.value) = 'string'
			AND NOT starts_with(resolved.value #>> '{}', filter_clause->>'value')
		ELSE false
	END
  );
--> statement-breakpoint
DROP FUNCTION "dispatch_migration_0018_resolve_json_pointer"(jsonb, text);
