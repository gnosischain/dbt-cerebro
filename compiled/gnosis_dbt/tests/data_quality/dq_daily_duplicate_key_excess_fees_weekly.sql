
-- The weekly fees table is ReplacingMergeTree read WITHOUT FINAL downstream, so a
-- duplicate copy doubles cohort fees/users one-for-one. Any nonzero excess means a
-- scoped append ran over populated months. Lesson: append-over-populated-duplicates
-- (2026-07 incident: every weekly cohort doubled at 2026-03-02).
SELECT
    toStartOfMonth(week) AS month,
    stream_type,
    count() - uniqExact(week, stream_type, symbol, user) AS dup_excess
FROM `dbt`.`int_revenue_fees_weekly_per_user`
GROUP BY month, stream_type
HAVING dup_excess > 0
ORDER BY dup_excess DESC