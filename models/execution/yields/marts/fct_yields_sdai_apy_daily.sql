{{ 
    config(
        materialized='view',
        tags=['production','execution','yields','sdai_apy']
    )
}}

WITH

sdai_apy_daily AS (
    SELECT
        date,
        floor(POWER((1+rate),365) - 1,4) * 100 AS apy,
        floor(
            avg(POWER((1+rate),365) - 1)
                OVER (ORDER BY date ROWS BETWEEN 6  PRECEDING AND CURRENT ROW)
            ,4) * 100 AS apy_7DMA,
        floor(
            avg(POWER((1+rate),365) - 1)
                OVER (ORDER BY date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW)
            ,4) * 100 AS apy_30DMA,
         floor(
            median(POWER((1+rate),365) - 1)
                OVER (ORDER BY date ROWS BETWEEN 6  PRECEDING AND CURRENT ROW)
            ,4) * 100 AS apy_7DMM,
        floor(
            median(POWER((1+rate),365) - 1)
                OVER (ORDER BY date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW)
            ,4) * 100 AS apy_30DMM
    FROM {{ ref('int_yields_sdai_rate_daily') }}
)

SELECT date, apy, 'Daily' AS label FROM sdai_apy_daily
UNION ALL
SELECT date, apy_7DMA, '7DMA' AS label FROM sdai_apy_daily
UNION ALL
SELECT date, apy_30DMA, '30DMA' AS label FROM sdai_apy_daily
UNION ALL
SELECT date, apy_7DMM, '7DMM' AS label FROM sdai_apy_daily
UNION ALL
SELECT date, apy_30DMM, '30DMM' AS label FROM sdai_apy_daily
