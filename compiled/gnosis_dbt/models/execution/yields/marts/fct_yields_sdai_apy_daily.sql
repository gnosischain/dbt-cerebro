

-- Legacy compatibility wrapper.
--
-- The canonical mart is fct_yields_savings_xdai_apy_daily. This wrapper preserves
-- the historical (date, apy, label) shape so existing dashboard queries and the
-- metrics-dashboard `historical_yield_sdai` metric keep working unchanged.

SELECT
    date,
    apy,
    label
FROM `dbt`.`fct_yields_savings_xdai_apy_daily`