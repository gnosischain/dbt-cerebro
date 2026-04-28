

-- Reverse lookup: "which Safes does this address own?" One row per
-- (owner, safe) pair enriched with that safe's threshold, owner count, and
-- deployment date so the Account Portfolio Safe section can render a full
-- table in a single round-trip.
--
-- Note: the filter column is `owner_address` so the same account search
-- input can cascade directly; the caller doesn't need to know the schema
-- of the underlying intermediate.
SELECT
    owner_address,
    safe_address,
    became_owner_at,
    current_threshold,
    current_owner_count,
    creation_version,
    deployment_date
FROM `dbt`.`fct_execution_account_safes_latest`