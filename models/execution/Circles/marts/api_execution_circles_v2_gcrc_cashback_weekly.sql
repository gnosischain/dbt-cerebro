{{
    config(
        materialized='view',
        tags=['production','execution','tier1','api:circles_v2_gcrc_cashback_weekly','granularity:weekly']
    )
}}

-- Weekly Circles v2 gCRC cashback distributed: distinct recipients and total
-- gCRC sent from the cashback wallet to app users (>= 1 gCRC/week threshold).
-- Latest incomplete week excluded.
SELECT
    week,
    count()      AS n_recipients,
    sum(amount)  AS amount
FROM {{ ref('int_execution_circles_v2_gcrc_cashback_recipients_weekly') }}
WHERE week < toStartOfWeek(today(), 1)
GROUP BY week
ORDER BY week
