WITH

pool_labels AS (
    SELECT DISTINCT
        protocol,
        pool_address,
        token,
        pool
    FROM `dbt`.`fct_execution_yields_pools_daily`
    WHERE token IS NOT NULL AND token != ''
      AND pool IS NOT NULL
),

lp_by_pool AS (
    SELECT
        d.date,
        pl.token,
        pl.pool AS label,
        sum(d.mint_count) AS mints,
        sum(d.burn_count) AS burns
    FROM `dbt`.`int_execution_yields_pools_lps_daily` d
    INNER JOIN pool_labels pl
        ON pl.pool_address = d.pool_address
        AND pl.protocol = d.protocol
    WHERE d.date < today()
    GROUP BY d.date, pl.token, pl.pool
)

SELECT date, token, label, 'Add' AS type, mints AS value
FROM lp_by_pool

UNION ALL

SELECT date, token, label, 'Remove' AS type, burns AS value
FROM lp_by_pool

ORDER BY date DESC, token, label, type