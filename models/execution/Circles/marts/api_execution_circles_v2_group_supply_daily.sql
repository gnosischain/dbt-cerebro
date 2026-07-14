{{
    config(
        materialized='view',
        tags=['production','execution','tier1','api:circles_v2_group_supply_daily','granularity:daily']
    )
}}

-- Per-group Circles v2 token supply over time (nominal + demurraged),
-- one row per (date, group). Uses the authoritative per-token supply from
-- tokens_supply_daily (zero-address minting), which already accounts for
-- the ERC-1155 held by ERC-20 wrappers -- wrapped tokens are a subset of
-- this supply, not additional, so we do NOT add wrapper supply on top.
-- The wrapped share is surfaced as a KPI from group_token_supply_current.
SELECT
    date,
    lower(token_address) AS group_address,
    supply,
    demurraged_supply AS supply_demurraged
FROM {{ ref('fct_execution_circles_v2_tokens_supply_daily') }}
WHERE date < today()
  AND lower(token_address) IN (
      SELECT DISTINCT lower(avatar)
      FROM {{ ref('int_execution_circles_v2_avatars') }}
      WHERE avatar_type = 'Group'
  )
