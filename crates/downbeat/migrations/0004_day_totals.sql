-- What pruning removed, per job and day (a calendar day in the definition's zone when it was
-- pruned), so the report still counts runs that are no longer in the history. Dry runs and joined
-- requests are not counted. A new table, so older engines ignore it.
CREATE TABLE downbeat.day_totals (
    job_name        text NOT NULL,
    day             date NOT NULL,
    succeeded       integer NOT NULL DEFAULT 0,
    failed          integer NOT NULL DEFAULT 0,
    skipped         integer NOT NULL DEFAULT 0,
    cancelled       integer NOT NULL DEFAULT 0,
    expired         integer NOT NULL DEFAULT 0,
    -- The longest succeeded run, from start to finish.
    longest_seconds double precision,
    PRIMARY KEY (job_name, day)
);
