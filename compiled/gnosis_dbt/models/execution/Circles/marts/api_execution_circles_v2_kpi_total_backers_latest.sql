

SELECT sub.*, (SELECT toDate(max(date)) FROM `dbt`.`fct_execution_circles_v2_backers_cumulative_daily`) AS as_of_date
FROM (
-- KPI tile: total trust-defined backers CURRENTLY trusted by the backers
-- group (var('circles_target_group_address')). The headline value counts only
-- currently-open trust intervals (backers_current.is_currently_trusted), so
-- untrusted backers drop off. The WoW delta uses the cumulative first-trusted
-- series as a growth proxy (untrust volume is small relative to new trusts).

WITH current_date_d AS (
    SELECT max(date) AS d
    FROM `dbt`.`fct_execution_circles_v2_backers_cumulative_daily`
    WHERE date < today()
),
current AS (
    SELECT countIf(is_currently_trusted) AS value
    FROM `dbt`.`int_execution_circles_v2_backers_current`
),
cum_now AS (
    SELECT cumulative_backers AS value
    FROM `dbt`.`fct_execution_circles_v2_backers_cumulative_daily`
    WHERE date = (SELECT d FROM current_date_d)
),
cum_prior AS (
    SELECT cumulative_backers AS value
    FROM `dbt`.`fct_execution_circles_v2_backers_cumulative_daily`
    WHERE date = (SELECT d FROM current_date_d) - 7
)

SELECT
    c.value                                                          AS value,
    round((n.value - p.value) / nullIf(p.value, 0) * 100, 1)         AS change_pct
FROM current c
CROSS JOIN cum_now n
CROSS JOIN cum_prior p
) AS sub