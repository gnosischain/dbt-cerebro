

WITH days AS (
    SELECT date, sum(n_claims) AS n_claims
    FROM `dbt`.`fct_execution_gnosis_app_token_offer_claims_daily`
    GROUP BY date
),
recent AS (SELECT sum(n_claims) AS v FROM days
           WHERE date >= today() - INTERVAL 7 DAY AND date < today()),
prior  AS (SELECT sum(n_claims) AS v FROM days
           WHERE date >= today() - INTERVAL 14 DAY AND date < today() - INTERVAL 7 DAY)
SELECT
    (SELECT v FROM recent)                                                AS value,
    round(((SELECT v FROM recent) - (SELECT v FROM prior))
          / nullIf((SELECT v FROM prior), 0) * 100, 1)                    AS change_pct