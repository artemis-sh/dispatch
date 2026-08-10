CREATE OR REPLACE FUNCTION dispatch_reject_expired_revision_resolution_failure()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  IF OLD.state = 'LEASED'
    AND OLD.lease_expires_at IS NOT NULL
    AND OLD.lease_expires_at <= clock_timestamp()
    AND NEW.state IN ('RETRY_WAIT', 'DEAD_LETTERED')
    AND NEW.lease_owner IS NULL
    AND NEW.lease_token IS NULL
    AND NEW.lease_expires_at IS NULL THEN
    RETURN NULL;
  END IF;

  RETURN NEW;
END;
$$;--> statement-breakpoint

CREATE TRIGGER dispatch_reject_expired_revision_resolution_failure
BEFORE UPDATE ON dispatch_event_revision_resolutions
FOR EACH ROW
EXECUTE FUNCTION dispatch_reject_expired_revision_resolution_failure();
