

WITH latest AS (
    SELECT max(date) AS d FROM `dbt`.`int_dao_treasury_holdings_daily`
),
current_val AS (
    SELECT sum(balance_usd) AS v
    FROM `dbt`.`int_dao_treasury_holdings_daily`
    WHERE date = (SELECT d FROM latest)
      AND position_type = 'lending'
),
prior_val AS (
    SELECT sum(balance_usd) AS v
    FROM `dbt`.`int_dao_treasury_holdings_daily`
    WHERE date = (SELECT d FROM latest) - INTERVAL 7 DAY
      AND position_type = 'lending'
)
SELECT
    round((SELECT v FROM current_val), 0) AS value,
    round(((SELECT v FROM current_val) - (SELECT v FROM prior_val))
          / nullIf((SELECT v FROM prior_val), 0) * 100, 1) AS change_pct