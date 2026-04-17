

-- Canonical Savings xDAI APY mart.
--
-- Long-format output with one row per (date, label) where label ∈
-- {'Daily','7DMA','30DMA','7DMM','30DMM'}. APY is compounded from `daily_rate`
-- (a proper ratio, not a diff — see int_yields_savings_xdai_rate_daily).
--
-- Regime columns travel through so consumers can distinguish pre-/post-2025-11-07
-- rows (DAI/sDAI vs USDS/sUSDS backing) without joining again.

WITH

apy_daily AS (
    SELECT
        date,
        canonical_label,
        legacy_symbol,
        backing_asset,
        yield_source,
        floor(POWER(1 + daily_rate, 365) - 1, 4) * 100 AS apy,
        floor(
            avg(POWER(1 + daily_rate, 365) - 1)
                OVER (ORDER BY date ROWS BETWEEN 6  PRECEDING AND CURRENT ROW)
        , 4) * 100 AS apy_7DMA,
        floor(
            avg(POWER(1 + daily_rate, 365) - 1)
                OVER (ORDER BY date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW)
        , 4) * 100 AS apy_30DMA,
        floor(
            median(POWER(1 + daily_rate, 365) - 1)
                OVER (ORDER BY date ROWS BETWEEN 6  PRECEDING AND CURRENT ROW)
        , 4) * 100 AS apy_7DMM,
        floor(
            median(POWER(1 + daily_rate, 365) - 1)
                OVER (ORDER BY date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW)
        , 4) * 100 AS apy_30DMM
    FROM `dbt`.`int_yields_savings_xdai_rate_daily`
)

SELECT date, apy,       'Daily' AS label, canonical_label, legacy_symbol, backing_asset, yield_source FROM apy_daily
UNION ALL
SELECT date, apy_7DMA,  '7DMA'  AS label, canonical_label, legacy_symbol, backing_asset, yield_source FROM apy_daily
UNION ALL
SELECT date, apy_30DMA, '30DMA' AS label, canonical_label, legacy_symbol, backing_asset, yield_source FROM apy_daily
UNION ALL
SELECT date, apy_7DMM,  '7DMM'  AS label, canonical_label, legacy_symbol, backing_asset, yield_source FROM apy_daily
UNION ALL
SELECT date, apy_30DMM, '30DMM' AS label, canonical_label, legacy_symbol, backing_asset, yield_source FROM apy_daily