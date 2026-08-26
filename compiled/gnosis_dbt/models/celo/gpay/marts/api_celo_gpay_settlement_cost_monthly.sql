

-- What it costs Gnosis Pay to settle Celo card charges, by month.
--
-- Aggregates the batch fact directly rather than sitting on a monthly fact model: the
-- source is 476 rows, so a separate materialisation would cost more to maintain than it
-- saves. Revisit if batch volume grows by an order of magnitude.
--
-- The ratios are computed in an outer SELECT because ClickHouse rejects an aggregate
-- inside another aggregate with code 184 (`nullIf(sum(x), 0)` nested under a sum).
SELECT
    toString(month)                                     AS month,
    batches,
    charges,
    failed_charges,
    settled_usd,
    fee_celo,
    fee_usd,
    round(fee_usd / nullIf(charges, 0), 5)              AS fee_usd_per_charge,
    round(fee_usd / nullIf(settled_usd, 0) * 10000, 2)  AS fee_bps_of_volume,
    round(charges / nullIf(batches, 0), 1)              AS charges_per_batch
FROM (
    SELECT
        toStartOfMonth(batch_date)     AS month,
        count()                        AS batches,
        sum(n_charges)                 AS charges,
        sum(n_failed_charges)          AS failed_charges,
        round(sum(settled_usd), 2)     AS settled_usd,
        round(sum(native_fee_celo), 2) AS fee_celo,
        round(sum(native_fee_usd), 2)  AS fee_usd
    FROM `dbt`.`fct_celo_gpay_settlement_batches`
    GROUP BY month
)
ORDER BY month