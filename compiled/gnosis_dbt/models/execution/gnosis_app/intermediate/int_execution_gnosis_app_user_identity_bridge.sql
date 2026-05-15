

-- INTERNAL ONLY — keeps raw `address` and `user_pseudonym` together so any
-- downstream model that needs to pseudonymize a raw address column can do
-- the join here instead of recomputing the hash. Never loaded by the MCP
-- semantic layer (filtered by tag:internal_only) and never exposed via
-- cerebro-api (no api:<resource> tag and the upcoming internal_only filter
-- would also catch it).
--
-- Same row count as int_execution_gnosis_app_users_current; same pseudonym
-- output as int_execution_gnosis_app_user_identities (deterministic macro).

SELECT
    lower(address)                          AS address,
    
    sipHash64(concat(unhex('00'), lower(address)))
   AS user_pseudonym,
    first_seen_at,
    last_seen_at,
    heuristic_kinds,
    heuristic_hits,
    n_distinct_heuristics
FROM `dbt`.`int_execution_gnosis_app_users_current`
WHERE address IS NOT NULL