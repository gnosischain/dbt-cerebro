

WITH

prev_balances AS (
    SELECT
        protocol,
        reserve_address,
        user_address,
        balance_usd AS balance_usd_7d_ago
    FROM `dbt`.`int_execution_lending_aave_user_balances_daily`
    WHERE date = (
        SELECT max(date) - 7
        FROM `dbt`.`int_execution_lending_aave_user_balances_daily`
        WHERE date < today() AND balance > 0
    )
      AND balance > 0
      AND (protocol, reserve_address, user_address) IN (
          SELECT protocol, reserve_address, user_address
          FROM `dbt`.`fct_execution_lending_top_lenders_ranked`
      )
)

SELECT
    r.rank,
    r.protocol AS protocol,
    r.reserve_address AS reserve_address,
    r.symbol,
    r.user_address AS user_address,
    l.project AS label,
    r.balance,
    r.balance_usd,
    round(r.pct_of_total, 4) AS pct_of_total,
    round(sum(r.pct_of_total) OVER (
        PARTITION BY r.protocol, r.symbol ORDER BY r.rank
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ), 4) AS cumulative_pct,
    r.balance_usd - coalesce(p.balance_usd_7d_ago, 0) AS change_usd_7d
FROM `dbt`.`fct_execution_lending_top_lenders_ranked` r
LEFT JOIN prev_balances p
    ON r.protocol = p.protocol
   AND r.reserve_address = p.reserve_address
   AND r.user_address = p.user_address
LEFT JOIN `dbt`.`int_crawlers_data_labels` l
    ON lower(l.address) = lower(r.user_address)
ORDER BY r.protocol, r.symbol, r.rank