

-- KPI tile: % of distinct depositors that are also trust-defined backers
-- (i.e. were eventually trusted by the backers group). The diagnostic
-- single number for the depositors-vs-backers gap.

WITH depositors AS (
    SELECT DISTINCT backer FROM `dbt`.`int_execution_circles_v2_backing_depositors_current`
),
backers AS (
    SELECT DISTINCT backer FROM `dbt`.`int_execution_circles_v2_backers_current`
),
overlap AS (
    SELECT
        count() AS total_depositors,
        countIf(b.backer IS NOT NULL) AS depositors_in_backers
    FROM depositors d
    LEFT JOIN backers b USING (backer)
)

SELECT
    round(depositors_in_backers / nullIf(total_depositors, 0) * 100, 1) AS value,
    total_depositors                                                    AS total_depositors,
    depositors_in_backers                                               AS depositors_in_backers,
    toFloat64(NULL)                                                     AS change_pct
FROM overlap