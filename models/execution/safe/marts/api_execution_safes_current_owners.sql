{{
    config(
        materialized='view',
        tags=["production", "execution", "safe", "tier2", "api:safes_current_owners", "granularity:latest"],
        meta={
            "api": {
                "methods": ["GET", "POST"],
                "allow_unfiltered": false,
                "require_any_of": ["safe_address", "owner_address"],
                "parameters": [
                    {
                        "name": "safe_address",
                        "column": "safe_address",
                        "operator": "=",
                        "type": "string",
                        "case": "lower",
                        "description": "Safe contract address — returns every current owner of this Safe"
                    },
                    {
                        "name": "owner_address",
                        "column": "owner_address",
                        "operator": "=",
                        "type": "string",
                        "case": "lower",
                        "description": "Owner (EOA or contract) address — returns every Safe this address currently owns"
                    }
                ],
                "pagination": {
                    "enabled": true,
                    "default_limit": 500,
                    "max_limit": 5000,
                    "response": "envelope"
                },
                "sort": [
                    {"column": "became_owner_at", "direction": "DESC"}
                ],
                "sortable_fields": ["safe_address", "owner_address", "became_owner_at"]
            }
        }
    )
}}

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
FROM {{ ref('int_execution_safes_current_owners') }}
