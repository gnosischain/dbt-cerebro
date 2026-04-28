{{
  config(
    materialized='view',
    tags=['production','execution','dao_treasury','kpi','tier1',
          'api:dao_treasury_kpi_gno_held','granularity:latest']
  )
}}

WITH latest AS (
    SELECT max(date) AS d FROM {{ ref('int_dao_treasury_holdings_daily') }}
),
current_val AS (
    SELECT sum(balance) AS v
    FROM {{ ref('int_dao_treasury_holdings_daily') }}
    WHERE date = (SELECT d FROM latest)
      AND symbol IN ('GNO', 'sGNO', 'spGNO', 'aGnoGNO')
),
prior_val AS (
    SELECT sum(balance) AS v
    FROM {{ ref('int_dao_treasury_holdings_daily') }}
    WHERE date = (SELECT d FROM latest) - INTERVAL 7 DAY
      AND symbol IN ('GNO', 'sGNO', 'spGNO', 'aGnoGNO')
)
SELECT
    round((SELECT v FROM current_val), 0) AS value,
    round(((SELECT v FROM current_val) - (SELECT v FROM prior_val))
          / nullIf((SELECT v FROM prior_val), 0) * 100, 1) AS change_pct
