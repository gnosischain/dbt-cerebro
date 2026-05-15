{{
  config(
    materialized='table',
    engine='MergeTree()',
    order_by='(kpi_name, media_name)',
    tags=['production', 'mmm', 'execution', 'mart']
  )
}}

-- Per-(KPI, media) baseline KPI: the median KPI value during weeks where
-- the media's adstocked spend sits in the bottom decile. This is what
-- mmm_analyst's SOP step 5 calls "baseline" — the level the KPI sits at
-- when the media is effectively zero, used to anchor the log-log
-- response curve fit.
--
-- Adstock decay rate λ = 0.5 is hardcoded as a default reference
-- (recent-week weight = 1, prior-week = 0.5, two-back = 0.25, …). The
-- persona can recompute with its own λ if needed; this mart is a
-- starting point.

WITH source AS (
  SELECT *
  FROM {{ ref('fct_execution_mmm_spine_weekly') }}
  WHERE week >= today() - INTERVAL 730 DAY
),

-- Compute geometric adstock for each media column (8-week window, λ=0.5)
adstocked AS (
  SELECT
    week,
    -- KPIs (passed through)
    kpi_pools_tvl_usd, kpi_pools_volume_usd, kpi_dex_volume_usd_dedup,
    kpi_gpay_active_users, kpi_gpay_payment_volume_usd,
    kpi_ga_active_users, kpi_ga_new_users,
    kpi_chain_tx_count, kpi_gpay_topups_count, kpi_gpay_topups_volume_usd,
    kpi_gno_staked, kpi_bridge_inflow_usd, kpi_bridge_outflow_usd,
    -- Media adstocks (running 8-week geometric decay)
    arraySum(arrayMap((x, i) -> x * pow(0.5, i),
      arrayReverse(groupArray(media_validator_proposer_rewards_gno) OVER (ORDER BY week ROWS BETWEEN 8 PRECEDING AND CURRENT ROW)),
      range(length(groupArray(media_validator_proposer_rewards_gno) OVER (ORDER BY week ROWS BETWEEN 8 PRECEDING AND CURRENT ROW)))
    )) AS media_validator_proposer_rewards_gno_adstock,

    arraySum(arrayMap((x, i) -> x * pow(0.5, i),
      arrayReverse(groupArray(media_ga_token_offer_emissions_usd) OVER (ORDER BY week ROWS BETWEEN 8 PRECEDING AND CURRENT ROW)),
      range(length(groupArray(media_ga_token_offer_emissions_usd) OVER (ORDER BY week ROWS BETWEEN 8 PRECEDING AND CURRENT ROW)))
    )) AS media_ga_token_offer_emissions_usd_adstock,

    arraySum(arrayMap((x, i) -> x * pow(0.5, i),
      arrayReverse(groupArray(media_pools_lp_fee_apr_avg) OVER (ORDER BY week ROWS BETWEEN 8 PRECEDING AND CURRENT ROW)),
      range(length(groupArray(media_pools_lp_fee_apr_avg) OVER (ORDER BY week ROWS BETWEEN 8 PRECEDING AND CURRENT ROW)))
    )) AS media_pools_lp_fee_apr_avg_adstock
  FROM source
),

long_kpi AS (
  SELECT week, 'pools_tvl_usd'              AS kpi_name, kpi_pools_tvl_usd              AS kpi_v FROM adstocked UNION ALL
  SELECT week, 'pools_volume_usd',                    kpi_pools_volume_usd            FROM adstocked UNION ALL
  SELECT week, 'dex_volume_usd_dedup',                kpi_dex_volume_usd_dedup        FROM adstocked UNION ALL
  SELECT week, 'gpay_topups_count',                   kpi_gpay_topups_count           FROM adstocked UNION ALL
  SELECT week, 'gpay_topups_volume_usd',              kpi_gpay_topups_volume_usd      FROM adstocked UNION ALL
  SELECT week, 'ga_active_users',                     kpi_ga_active_users             FROM adstocked UNION ALL
  SELECT week, 'ga_new_users',                        kpi_ga_new_users                FROM adstocked
),

long_media_adstock AS (
  SELECT week, 'validator_proposer_rewards_gno'  AS media_name, media_validator_proposer_rewards_gno_adstock  AS adstock_v FROM adstocked UNION ALL
  SELECT week, 'ga_token_offer_emissions_usd',                  media_ga_token_offer_emissions_usd_adstock     FROM adstocked UNION ALL
  SELECT week, 'pools_lp_fee_apr_avg',                          media_pools_lp_fee_apr_avg_adstock              FROM adstocked
),

joined AS (
  SELECT
    k.kpi_name,
    m.media_name,
    k.kpi_v,
    m.adstock_v
  FROM long_kpi k
  INNER JOIN long_media_adstock m USING (week)
  WHERE k.kpi_v IS NOT NULL AND m.adstock_v IS NOT NULL
),

thresholds AS (
  SELECT
    kpi_name,
    media_name,
    quantile(0.1)(adstock_v) AS bottom_decile_threshold
  FROM joined
  GROUP BY kpi_name, media_name
)

SELECT
  j.kpi_name                            AS kpi_name,
  j.media_name                          AS media_name,
  quantile(0.5)(j.kpi_v)                AS baseline_kpi_median,
  quantile(0.05)(j.kpi_v)               AS baseline_kpi_q05,
  quantile(0.95)(j.kpi_v)               AS baseline_kpi_q95,
  any(t.bottom_decile_threshold)        AS bottom_decile_threshold,
  count()                               AS n_low_spend_weeks,
  now()                                 AS computed_at
FROM joined j
INNER JOIN thresholds t USING (kpi_name, media_name)
WHERE j.adstock_v <= t.bottom_decile_threshold
GROUP BY j.kpi_name, j.media_name
HAVING n_low_spend_weeks > 5
