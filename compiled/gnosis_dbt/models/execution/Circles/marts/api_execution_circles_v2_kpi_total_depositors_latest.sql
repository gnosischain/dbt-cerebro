

SELECT sub.*, (SELECT toDate(max(block_timestamp)) FROM `dbt`.`int_execution_circles_v2_backing`) AS as_of_date
FROM (
-- KPI tile: total distinct depositors (addresses that have emitted a
-- CirclesBackingInitiated event). Distinct from "backers" — see
-- int_execution_circles_v2_backers_current. The 7-day delta counts
-- depositors whose first initiation was in the last 7 days.

WITH base AS (
    SELECT
        count() AS value,
        countIf(first_initiated_at > now() - INTERVAL 7 DAY) AS new_last_7d,
        countIf(first_initiated_at > now() - INTERVAL 14 DAY
                AND first_initiated_at <= now() - INTERVAL 7 DAY) AS new_prior_7d
    FROM `dbt`.`int_execution_circles_v2_backing_depositors_current`
)

SELECT
    value                                                            AS value,
    new_last_7d                                                      AS new_last_7d,
    round((new_last_7d - new_prior_7d) / nullIf(new_prior_7d, 0) * 100, 1) AS change_pct
FROM base
) AS sub