

-- Cumulative Circles v2 gCRC cashback over time: running total gCRC and
-- running count of distinct lifetime recipients (each recipient counted the
-- week they first received cashback).
WITH weekly AS (
    SELECT week, sum(amount) AS amount
    FROM `dbt`.`int_execution_circles_v2_gcrc_cashback_recipients_weekly`
    WHERE week < toStartOfWeek(today(), 1)
    GROUP BY week
),
firsts AS (
    SELECT first_week, count() AS new_recipients
    FROM (
        SELECT address, min(week) AS first_week
        FROM `dbt`.`int_execution_circles_v2_gcrc_cashback_recipients_weekly`
        WHERE week < toStartOfWeek(today(), 1)
        GROUP BY address
    )
    GROUP BY first_week
)
SELECT
    w.week,
    sum(w.amount) OVER (ORDER BY w.week ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_amount,
    sum(coalesce(f.new_recipients, 0)) OVER (ORDER BY w.week ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_recipients
FROM weekly w
LEFT JOIN firsts f ON f.first_week = w.week
ORDER BY w.week