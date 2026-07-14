{{
    config(
        materialized='view',
        tags=['production','execution','tier1','api:circles_v2_gcrc_cashback_total','granularity:latest']
    )
}}

-- Lifetime Circles v2 gCRC cashback totals (single row) for KPI tiles.
SELECT
    today()             AS as_of_date,
    sum(amount)         AS total_amount,
    uniqExact(address)  AS total_recipients
FROM {{ ref('int_execution_circles_v2_gcrc_cashback_recipients_weekly') }}
WHERE week < toStartOfWeek(today(), 1)
