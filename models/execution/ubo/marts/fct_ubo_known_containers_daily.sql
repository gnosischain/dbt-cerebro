{{
    config(
        materialized='table',
        engine='ReplacingMergeTree()',
        order_by='(date, container_address, token_address)',
        partition_by='toStartOfMonth(date)',
        settings={'allow_nullable_key': 1},
        tags=['dev','execution','ubo','known_containers']
    )
}}

-- Distinct list of (date, container_address, token_address) tuples for
-- which we have UBO-level supply claims. Derived directly from
-- fct_ubo_supply_claims_daily so it stays self-consistent: a container
-- appears here iff we can decompose it.
--
-- Downstream consumers (top holders, UBO coverage) LEFT ANTI JOIN this
-- against balances_daily to strip out container-level rows before merging
-- in the per-UBO rows from fct_ubo_supply_claims_daily. Small — bounded by
-- (# protocols × # reserves × days) — so refresh is cheap.

SELECT DISTINCT
    date,
    container_address,
    token_address
FROM {{ ref('fct_ubo_supply_claims_daily') }}
WHERE date < today()
