

-- Cumulative conversion funnel: for each funnel and step, the number of
-- distinct users who reached at least that step (from the user-grain
-- windowFunnel fact). Backs the "Conversion Funnel" chart.
WITH per_user AS (
    SELECT funnel_name, user_pseudonym, max(level) AS max_level
    FROM `dbt`.`fct_execution_gnosis_app_funnel_daily`
    GROUP BY funnel_name, user_pseudonym
),
steps AS (
    SELECT DISTINCT funnel_name, level
    FROM `dbt`.`fct_execution_gnosis_app_funnel_daily`
)
SELECT
    today() AS as_of_date,
    s.funnel_name,
    s.level,
    concat('Step ', toString(s.level)) AS step_label,
    count(DISTINCT p.user_pseudonym)    AS n_users
FROM steps s
INNER JOIN per_user p
    ON p.funnel_name = s.funnel_name
   AND p.max_level >= s.level
GROUP BY s.funnel_name, s.level
ORDER BY s.funnel_name, s.level