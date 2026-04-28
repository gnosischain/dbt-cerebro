

-- Per-Safe current owners (add-only snapshot from the owner-events log).
-- One row per (safe, owner) pair where the last observed event for that pair
-- is `safe_setup` or `added_owner`. Matches the shape of every other latest
-- snapshot view: filter supported on either side of the relationship so the
-- Account Portfolio tab can query "who owns this safe?" and "which safes
-- does this address own?" against the same view.
--
-- Renaming `owner` → `owner_address` so the API filter parameter name reads
-- naturally. `current_threshold` surfaces here too so callers don't need a
-- second query to render "M of N signatures" — the threshold is constant
-- per safe and denormalising it per owner row avoids an extra join.
SELECT
    safe_address,
    owner AS owner_address,
    became_owner_at,
    current_threshold
FROM `dbt`.`int_execution_safes_current_owners`