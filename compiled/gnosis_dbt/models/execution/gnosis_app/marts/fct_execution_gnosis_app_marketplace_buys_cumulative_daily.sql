

WITH

-- Per-(date, offer) buy counts.
daily AS (
    SELECT
        toDate(block_timestamp)              AS date,
        offer_name                           AS offer_name,
        count(*)                             AS n_buys,
        sum(amount)                          AS volume_token
    FROM `dbt`.`int_execution_gnosis_app_marketplace_payments`
    GROUP BY date, offer_name
),

-- First-buy date per (offer, payer) — used to count distinct cumulative
-- payers per offer via a simple sum over first-appearance dates.
first_buy_per_user AS (
    SELECT
        offer_name,
        payer,
        min(toDate(block_timestamp))         AS first_buy_date
    FROM `dbt`.`int_execution_gnosis_app_marketplace_payments`
    GROUP BY offer_name, payer
),

new_payers_daily AS (
    SELECT
        first_buy_date                       AS date,
        offer_name,
        count(DISTINCT payer)                AS n_new_payers
    FROM first_buy_per_user
    GROUP BY first_buy_date, offer_name
),

-- Dense spine (offer_name × date) so cumulative series stays continuous.
date_bounds AS (
    SELECT min(date) AS min_date, today() AS max_date FROM daily
),
calendar AS (
    SELECT addDays(min_date, number) AS date
    FROM date_bounds
    ARRAY JOIN range(0, toUInt64(dateDiff('day', min_date, max_date) + 1)) AS number
),
offers AS (
    SELECT DISTINCT offer_name FROM daily
),
spine AS (
    SELECT c.date, o.offer_name
    FROM calendar c CROSS JOIN offers o
)

SELECT
    s.date                                                       AS date,
    s.offer_name                                                 AS offer_name,
    coalesce(d.n_buys, 0)                                        AS n_buys,
    coalesce(np.n_new_payers, 0)                                 AS n_new_payers,
    sum(coalesce(d.n_buys, 0))
        OVER (PARTITION BY s.offer_name ORDER BY s.date
              ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)  AS cumulative_buys,
    sum(coalesce(np.n_new_payers, 0))
        OVER (PARTITION BY s.offer_name ORDER BY s.date
              ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)  AS cumulative_payers
FROM spine s
LEFT JOIN daily d
    ON d.date = s.date AND d.offer_name = s.offer_name
LEFT JOIN new_payers_daily np
    ON np.date = s.date AND np.offer_name = s.offer_name
ORDER BY s.offer_name, s.date