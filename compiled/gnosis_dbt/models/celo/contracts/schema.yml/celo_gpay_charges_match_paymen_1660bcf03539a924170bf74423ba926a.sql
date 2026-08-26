

WITH bound AS (
    SELECT least(
        (SELECT max(block_time) FROM `dbt`.`int_celo_gpay_activity`),
        (SELECT max(block_timestamp)
         FROM `dbt`.`contracts_celo_gpay_settlement_events`
         WHERE event_name = 'TokenPullSuccess')
    ) AS shared_wm
),

counts AS (
    SELECT
        (SELECT count()
         FROM `dbt`.`contracts_celo_gpay_settlement_events`
         WHERE event_name = 'TokenPullSuccess'
           AND block_timestamp <= (SELECT shared_wm FROM bound)) AS pulls,
        (SELECT count()
         FROM `dbt`.`int_celo_gpay_activity`
         WHERE action = 'Payment'
           AND block_time <= (SELECT shared_wm FROM bound)) AS payments,
        (SELECT shared_wm FROM bound) AS shared_wm
)

SELECT
    payments,
    pulls,
    payments - pulls AS delta,
    shared_wm
FROM counts
WHERE payments != pulls

