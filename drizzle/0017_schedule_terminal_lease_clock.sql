CREATE OR REPLACE FUNCTION dispatch_reject_expired_schedule_occurrence_terminal_update()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  IF OLD.state = 'LEASED'
    AND OLD.lease_expires_at <= clock_timestamp()
    AND NEW.state IN ('SUCCEEDED', 'RETRY_WAIT', 'DEAD_LETTERED') THEN
    RETURN NULL;
  END IF;
  RETURN NEW;
END;
$$;
--> statement-breakpoint
CREATE TRIGGER dispatch_schedule_occurrence_terminal_lease_guard
BEFORE UPDATE ON dispatch_schedule_occurrences
FOR EACH ROW
EXECUTE FUNCTION dispatch_reject_expired_schedule_occurrence_terminal_update();
