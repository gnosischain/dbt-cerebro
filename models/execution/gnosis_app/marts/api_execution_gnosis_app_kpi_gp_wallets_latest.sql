{{
  config(
    materialized='view',
    tags=['production','execution','gnosis_app','kpi','tier0',
          'api:gnosis_app_kpi_gp_wallets','granularity:snapshot']
  )
}}

SELECT sub.*, (SELECT toDate(max(block_timestamp)) FROM {{ ref('int_execution_safes_module_events') }}) AS as_of_date
FROM (
SELECT
    -- Count distinct CANONICAL cards among GA-owned safes: a migrated pair (old + inherited new)
    -- is one card, so collapse old->new to avoid double-counting (see gpay_wallets_daily).
    uniqExactIf(if(c.canonical_address != '', c.canonical_address, w.pay_wallet),
                w.is_currently_ga_owned)              AS value,
    CAST(NULL AS Nullable(Float64))                   AS change_pct
FROM {{ ref('int_execution_gnosis_app_gpay_wallets') }} w
LEFT JOIN {{ ref('int_execution_gpay_safe_canonical') }} c ON c.address = w.pay_wallet
) AS sub
