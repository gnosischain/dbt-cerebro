{{
  config(
    materialized='incremental',
    incremental_strategy=('append' if start_month else 'delete+insert'),
    engine='ReplacingMergeTree()',
    order_by='(week, media_name)',
    unique_key='(week, media_name)',
    partition_by='toStartOfMonth(week)',
    settings={'allow_nullable_key': 1},
    tags=['production', 'mmm', 'execution', 'incentives']
  )
}}
{% set start_month = var('start_month', none) %}
{% set end_month   = var('end_month',   none) %}

-- MMM media (incentive / reward / outlay) variables on a continuous
-- weekly spine. Long-form: one row per (week, media_name). Missing
-- weeks filled with 0 (sum-method outlays) or NULL (apr/proxy
-- snapshots).
--
-- New media: add to seed mmm_media_registry.csv AND add an aggregator
-- CTE here.

WITH spine AS (
  {{ weekly_spine("today() - INTERVAL 730 DAY", "today() - INTERVAL 7 DAY") }}
),

-- ── Media source aggregators (weekly grain) ──────────────────────────

media_validator_proposer_rewards AS (
  SELECT
    toStartOfWeek(date, 1)                                              AS week,
    'validator_proposer_rewards_gno'                                    AS media_name,
    sum(proposer_reward_gno)                                            AS media_value,
    'sum'                                                               AS media_value_method,
    'int_consensus_validators_proposer_rewards_daily'                   AS source_model
  FROM {{ ref('int_consensus_validators_proposer_rewards_daily') }}
  GROUP BY week
),

media_ga_token_offer_emissions AS (
  SELECT
    toStartOfWeek(toDate(block_timestamp), 1)                           AS week,
    'ga_token_offer_emissions_usd'                                      AS media_name,
    sum(toFloat64OrNull(toString(amount_received_usd)))                 AS media_value,
    'sum'                                                               AS media_value_method,
    'int_execution_gnosis_app_token_offer_claims'                       AS source_model
  FROM {{ ref('int_execution_gnosis_app_token_offer_claims') }}
  GROUP BY week
),

media_pools_lp_fee_apr AS (
  -- Weighted-by-TVL average of pool fee APR. weighted_avg method.
  SELECT
    toStartOfWeek(date, 1)                                              AS week,
    'pools_lp_fee_apr_avg'                                              AS media_name,
    sum(fee_apr_7d * tvl_usd) / nullIf(sum(tvl_usd), 0)                 AS media_value,
    'weighted_avg'                                                      AS media_value_method,
    'int_execution_pools_metrics_daily'                                 AS source_model
  FROM {{ ref('int_execution_pools_metrics_daily') }}
  WHERE fee_apr_7d IS NOT NULL AND tvl_usd > 0
  GROUP BY week
),

all_feeds AS (
  SELECT * FROM media_validator_proposer_rewards
  UNION ALL SELECT * FROM media_ga_token_offer_emissions
  UNION ALL SELECT * FROM media_pools_lp_fee_apr
),

registry AS (
  SELECT media_name, media_value_method, media_units, is_outlay, source_model
  FROM {{ ref('mmm_media_registry') }}
),

filled AS (
  SELECT
    s.week                                                              AS week,
    r.media_name                                                        AS media_name,
    coalesce(
      f.media_value,
      if(r.media_value_method = 'sum', toFloat64(0), CAST(NULL AS Nullable(Float64)))
    )                                                                   AS media_value,
    r.media_value_method                                                AS media_value_method,
    r.media_units                                                       AS media_units,
    r.is_outlay                                                         AS is_outlay,
    r.source_model                                                      AS provenance_model
  FROM spine s
  CROSS JOIN registry r
  LEFT JOIN all_feeds f
    ON f.week = s.week AND f.media_name = r.media_name
)

SELECT * FROM filled
WHERE 1=1
{% if start_month and end_month %}
  AND toStartOfMonth(week) >= toDate('{{ start_month }}')
  AND toStartOfMonth(week) <= toDate('{{ end_month }}')
{% else %}
  {{ apply_monthly_incremental_filter('week', 'week', add_and=True) }}
{% endif %}
