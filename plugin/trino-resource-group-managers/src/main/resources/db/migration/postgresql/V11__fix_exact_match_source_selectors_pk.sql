ALTER TABLE exact_match_source_selectors
    DROP CONSTRAINT exact_match_source_selectors_pkey,
    ADD COLUMN id BIGSERIAL PRIMARY KEY,
    ALTER COLUMN environment DROP NOT NULL;
