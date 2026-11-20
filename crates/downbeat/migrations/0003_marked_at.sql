-- When a person marked a failed run as succeeded, beside who did it. `on_finish` counts its day
-- from this moment, so a run marked long after it ended is still delivered. Nullable, so older
-- engines ignore it.
ALTER TABLE downbeat.run_history ADD COLUMN marked_at timestamptz;
