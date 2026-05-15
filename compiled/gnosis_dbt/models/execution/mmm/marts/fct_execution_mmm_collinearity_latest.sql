

-- Pairwise Pearson correlation between every pair of media columns over
-- the last 730 days of the MMM spine. Refreshed daily. The MMM analyst
-- reads this directly instead of recomputing on every run; flags any
-- (col_a, col_b) pair with abs(pearson_corr) > 0.9 for the
-- multicollinearity check in mmm_analyst's SOP.
--
-- Cross-product is bounded by the registry seed length (~10 media), so
-- the output is at most ~100 rows. Trivial cost.

WITH source AS (
  SELECT *
  FROM `dbt`.`fct_execution_mmm_spine_weekly`
  WHERE week >= today() - INTERVAL 730 DAY
),

pairs AS (
  -- Self-correlation for diagnostic completeness; downstream code
  -- filters col_a < col_b for unique pairs.
  SELECT 'media_validator_proposer_rewards_gno' AS col_name, media_validator_proposer_rewards_gno AS v, week FROM source UNION ALL
  SELECT 'media_validator_income_gno',                       media_validator_income_gno,           week FROM source UNION ALL
  SELECT 'media_validator_apr_proxy',                        media_validator_apr_proxy,            week FROM source UNION ALL
  SELECT 'media_ga_token_offer_emissions_usd',               media_ga_token_offer_emissions_usd,   week FROM source UNION ALL
  SELECT 'media_gpay_cashback_outlay_usd',                   media_gpay_cashback_outlay_usd,       week FROM source UNION ALL
  SELECT 'media_pools_lp_fee_apr_avg',                       media_pools_lp_fee_apr_avg,           week FROM source UNION ALL
  SELECT 'media_lm_rewards_outlay_usd',                      media_lm_rewards_outlay_usd,          week FROM source UNION ALL
  SELECT 'media_bridge_incentive_outlay_usd',                media_bridge_incentive_outlay_usd,    week FROM source
)

SELECT
  a.col_name                                                          AS col_a,
  b.col_name                                                          AS col_b,
  corr(a.v, b.v)                                                      AS pearson_corr,
  count()                                                             AS n_weeks,
  toUInt8(abs(corr(a.v, b.v)) > 0.9)                                  AS is_high_collinearity,
  now()                                                               AS computed_at
FROM pairs a
INNER JOIN pairs b ON a.week = b.week
WHERE a.col_name < b.col_name              -- unique pairs only
  AND a.v IS NOT NULL AND b.v IS NOT NULL
GROUP BY col_a, col_b
ORDER BY abs(pearson_corr) DESC