-- A pause of everything: kind 'all', name 'all'. Older engines ignore such a row, so this is
-- additive: they keep claiming until they are replaced.
ALTER TABLE downbeat.pause DROP CONSTRAINT pause_kind_check;
ALTER TABLE downbeat.pause ADD CONSTRAINT pause_kind_check CHECK (kind IN ('schedule', 'job', 'queue', 'all'));
