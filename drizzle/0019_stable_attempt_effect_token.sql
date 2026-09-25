ALTER TABLE "dispatch_execution_attempts" ADD COLUMN "effect_token" text;
--> statement-breakpoint
UPDATE "dispatch_execution_attempts" SET "effect_token" = "fencing_token";
--> statement-breakpoint
ALTER TABLE "dispatch_execution_attempts" ALTER COLUMN "effect_token" SET NOT NULL;
--> statement-breakpoint
CREATE UNIQUE INDEX "dispatch_execution_attempts_effect_token_unique" ON "dispatch_execution_attempts" USING btree ("effect_token");
