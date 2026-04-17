

-- Legacy compatibility wrapper.
--
-- The canonical model is int_yields_savings_xdai_rate_daily. This wrapper preserves
-- the historical (date, sdai_conversion, rate) shape so existing downstream consumers
-- (fct_yields_sdai_apy_daily, overview snapshot, semantic layer) keep working without
-- a ref() churn. New work should read the canonical model directly to also get
-- canonical_label / backing_asset / yield_source regime columns.

SELECT
    date,
    share_price AS sdai_conversion,
    daily_rate  AS rate
FROM `dbt`.`int_yields_savings_xdai_rate_daily`
WHERE daily_rate IS NOT NULL