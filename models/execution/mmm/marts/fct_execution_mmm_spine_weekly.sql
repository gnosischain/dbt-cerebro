{{
  config(
    materialized='table',
    engine='MergeTree()',
    order_by='(week)',
    settings={'allow_nullable_key': 1},
    tags=['production', 'mmm', 'execution', 'mart']
  )
}}

-- MMM weekly spine — wide pivot of int_execution_mmm_kpis_weekly +
-- int_execution_mmm_media_weekly + int_execution_mmm_controls_weekly.
-- One row per week, one column per (kpi|media|control) name.
--
-- This is the single mart the MMM analyst's runtime mapping points at.
-- Persona reads it directly and runs adstock / Hill / contribution-decomp
-- against `media_*` columns + `kpi_*` columns + `ctrl_*` columns.
--
-- Adding a new KPI / media / control: update the corresponding
-- intermediate's UNION + the registry seed, then add a `sumIf` clause
-- here for the new name. Schema is wide-by-design and bounded by the
-- registry seeds (~30 cols total today).

WITH
kpis AS (
  SELECT week, kpi_name, kpi_value
  FROM {{ ref('int_execution_mmm_kpis_weekly') }}
),
media AS (
  SELECT week, media_name, media_value
  FROM {{ ref('int_execution_mmm_media_weekly') }}
),
ctrls AS (
  SELECT week, control_name, control_value
  FROM {{ ref('int_execution_mmm_controls_weekly') }}
),

weeks AS (
  SELECT DISTINCT week FROM kpis
  UNION DISTINCT
  SELECT DISTINCT week FROM media
  UNION DISTINCT
  SELECT DISTINCT week FROM ctrls
)

SELECT
  w.week                                                              AS week,
  -- ── KPIs ────────────────────────────────────────────────────────
  sumIf(k.kpi_value, k.kpi_name = 'pools_tvl_usd')                    AS kpi_pools_tvl_usd,
  sumIf(k.kpi_value, k.kpi_name = 'pools_volume_usd')                 AS kpi_pools_volume_usd,
  sumIf(k.kpi_value, k.kpi_name = 'dex_volume_usd_dedup')             AS kpi_dex_volume_usd_dedup,
  sumIf(k.kpi_value, k.kpi_name = 'gpay_active_users')                AS kpi_gpay_active_users,
  sumIf(k.kpi_value, k.kpi_name = 'gpay_payment_volume_usd')          AS kpi_gpay_payment_volume_usd,
  sumIf(k.kpi_value, k.kpi_name = 'ga_active_users')                  AS kpi_ga_active_users,
  sumIf(k.kpi_value, k.kpi_name = 'ga_new_users')                     AS kpi_ga_new_users,
  sumIf(k.kpi_value, k.kpi_name = 'chain_tx_count')                   AS kpi_chain_tx_count,
  sumIf(k.kpi_value, k.kpi_name = 'gpay_topups_count')                AS kpi_gpay_topups_count,
  sumIf(k.kpi_value, k.kpi_name = 'gpay_topups_volume_usd')           AS kpi_gpay_topups_volume_usd,
  sumIf(k.kpi_value, k.kpi_name = 'gno_staked')                       AS kpi_gno_staked,
  sumIf(k.kpi_value, k.kpi_name = 'bridge_inflow_usd')                AS kpi_bridge_inflow_usd,
  sumIf(k.kpi_value, k.kpi_name = 'bridge_outflow_usd')               AS kpi_bridge_outflow_usd,
  -- ── Media ───────────────────────────────────────────────────────
  sumIf(m.media_value, m.media_name = 'validator_proposer_rewards_gno') AS media_validator_proposer_rewards_gno,
  sumIf(m.media_value, m.media_name = 'validator_income_gno')         AS media_validator_income_gno,
  sumIf(m.media_value, m.media_name = 'validator_apr_proxy')          AS media_validator_apr_proxy,
  sumIf(m.media_value, m.media_name = 'ga_token_offer_emissions_usd') AS media_ga_token_offer_emissions_usd,
  sumIf(m.media_value, m.media_name = 'gpay_cashback_outlay_usd')     AS media_gpay_cashback_outlay_usd,
  sumIf(m.media_value, m.media_name = 'pools_lp_fee_apr_avg')         AS media_pools_lp_fee_apr_avg,
  sumIf(m.media_value, m.media_name = 'lm_rewards_outlay_usd')        AS media_lm_rewards_outlay_usd,
  sumIf(m.media_value, m.media_name = 'bridge_incentive_outlay_usd')  AS media_bridge_incentive_outlay_usd,
  -- ── Controls ────────────────────────────────────────────────────
  sumIf(c.control_value, c.control_name = 'gno_usd_price_avg')        AS ctrl_gno_usd_price_avg,
  sumIf(c.control_value, c.control_name = 'eth_usd_price_avg')        AS ctrl_eth_usd_price_avg,
  sumIf(c.control_value, c.control_name = 'wxdai_eur_proxy_avg')      AS ctrl_wxdai_eur_proxy_avg,
  sumIf(c.control_value, c.control_name = 'chain_gas_price_gwei_avg') AS ctrl_chain_gas_price_gwei_avg,
  sumIf(c.control_value, c.control_name = 'chain_block_count')        AS ctrl_chain_block_count,
  sumIf(c.control_value, c.control_name = 'week_of_year')             AS ctrl_week_of_year,
  sumIf(c.control_value, c.control_name = 'is_holiday_week')          AS ctrl_is_holiday_week,
  sumIf(c.control_value, c.control_name = 'week_index')               AS ctrl_week_index,
  sumIf(c.control_value, c.control_name = 'hardfork_step')            AS ctrl_hardfork_step
FROM weeks w
LEFT JOIN kpis  k ON k.week = w.week
LEFT JOIN media m ON m.week = w.week
LEFT JOIN ctrls c ON c.week = w.week
GROUP BY w.week
ORDER BY w.week
