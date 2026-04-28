{{
    config(
        materialized='view',
        tags=["production", "execution", "safe", "tier2", "api:account_safes", "granularity:latest"],
        meta={
            "api": {
                "methods": ["GET", "POST"],
                "allow_unfiltered": false,
                "require_any_of": ["owner_address"],
                "parameters": [
                    {
                        "name": "owner_address",
                        "column": "owner_address",
                        "operator": "=",
                        "type": "string",
                        "case": "lower",
                        "description": "Address whose owned Safes we want to list"
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
                "sortable_fields": ["owner_address", "safe_address", "became_owner_at", "current_threshold", "current_owner_count", "deployment_date"]
            }
        }
    )
}}

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
FROM {{ ref('fct_execution_account_safes_latest') }}
