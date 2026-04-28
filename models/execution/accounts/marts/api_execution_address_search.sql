{{
    config(
        materialized='view',
        tags=['production', 'execution', 'accounts', 'tier1', 'api:address_search', 'granularity:latest'],
        meta={
            "api": {
                "methods": ["GET", "POST"],
                "allow_unfiltered": true,
                "pagination": {
                    "enabled": true,
                    "default_limit": 10000,
                    "max_limit": 50000,
                    "response": "envelope"
                },
                "sort": [
                    {"column": "connected_validator_count", "direction": "DESC"},
                    {"column": "connected_safe_count", "direction": "DESC"}
                ],
                "sortable_fields": ["address", "display_name", "connected_validator_count", "connected_safe_count"]
            }
        }
    )
}}

-- Lightweight dropdown source for the Account Portfolio tab's global
-- filter. Same shape as `api_consensus_validators_search` and
-- `api_execution_circles_v2_avatar_search`: two columns, address + display_name.
-- LabelSelector substring-matches client-side so a user pasting a raw hex
-- address or typing a Circles handle both work with one request.
--
-- HIGH-SIGNAL ONLY: from the ~1M addresses in the resolver, include only
-- addresses findable by typing — Circles handle, validator-operator
-- withdrawal_address, or owner of ≥2 Safes. Bare Safe contracts and
-- 1-safe-owner EOAs are excluded because they have no human-readable
-- identifier to search by.
--
-- The resolver fct now stores one row per (address × source). We:
--   1. Pre-filter to high-signal source rows — tiny set (~5-10k addresses)
--   2. Do the per-address merge ONLY on that filtered set
-- This avoids a full-table GROUP BY which would OOM the cluster.
WITH high_signal_addresses AS (
  SELECT DISTINCT address
  FROM {{ ref('fct_execution_address_resolver') }}
  WHERE is_circles_avatar > 0
     OR is_validator_withdrawal_address > 0
     OR (is_safe_owner > 0 AND connected_safe_count >= 2)
),

merged AS (
  SELECT
    address,
    max(is_safe) > 0                          AS is_safe_m,
    max(is_safe_owner) > 0                    AS is_safe_owner_m,
    max(is_circles_avatar) > 0                AS is_circles_avatar_m,
    max(is_gpay_wallet) > 0                   AS is_gpay_wallet_m,
    max(connected_safe_count)                 AS connected_safe_m,
    max(connected_validator_count)            AS connected_validator_m,
    max(circles_name)                         AS circles_name_m
  FROM {{ ref('fct_execution_address_resolver') }}
  WHERE address IN (SELECT address FROM high_signal_addresses)
  GROUP BY address
)

SELECT
  address,
  COALESCE(
    circles_name_m,
    multiIf(
      connected_validator_m > 0,
        concat('Validator operator · ', toString(connected_validator_m), ' validators'),
      connected_safe_m > 0,
        concat('Safe owner · ', toString(connected_safe_m), ' safes'),
      is_safe_m,           'Safe contract',
      is_gpay_wallet_m,    'Gnosis Pay wallet',
      is_circles_avatar_m, 'Circles avatar',
      ''
    )
  ) AS display_name,
  toUInt64(connected_validator_m) AS connected_validator_count,
  toUInt64(connected_safe_m)      AS connected_safe_count
FROM merged
