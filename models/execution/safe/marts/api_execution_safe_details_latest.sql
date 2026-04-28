{{
    config(
        materialized='view',
        tags=["production", "execution", "safe", "tier2", "api:safe_details", "granularity:latest"],
        meta={
            "api": {
                "methods": ["GET", "POST"],
                "allow_unfiltered": false,
                "require_any_of": ["safe_address"],
                "parameters": [
                    {
                        "name": "safe_address",
                        "column": "safe_address",
                        "operator": "=",
                        "type": "string",
                        "case": "lower",
                        "description": "Safe contract address"
                    }
                ],
                "pagination": {
                    "enabled": true,
                    "default_limit": 100,
                    "max_limit": 1000,
                    "response": "envelope"
                }
            }
        }
    )
}}

-- One row per Safe address: deployment info, current threshold, and
-- current-owner count. Powers the Safe-section summary card in the Account
-- Portfolio tab without needing a second aggregation round-trip.
WITH owners AS (
    SELECT
        safe_address,
        count() AS current_owner_count,
        any(current_threshold) AS current_threshold
    FROM {{ ref('int_execution_safes_current_owners') }}
    GROUP BY safe_address
)

SELECT
    s.safe_address AS safe_address,
    s.creation_version AS creation_version,
    s.is_l2 AS is_l2,
    s.creation_singleton AS creation_singleton,
    s.block_date AS deployment_date,
    s.block_timestamp AS deployment_timestamp,
    s.tx_hash AS deployment_tx_hash,
    COALESCE(o.current_owner_count, 0) AS current_owner_count,
    o.current_threshold AS current_threshold
FROM {{ ref('int_execution_safes') }} AS s
LEFT JOIN owners AS o USING (safe_address)
