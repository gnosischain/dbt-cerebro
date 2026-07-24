
-- Accounting identity: per-user aToken scaled_balance is BY CONSTRUCTION the cumulative sum
-- of diff_scaled, so for each (protocol, reserve) the day-over-day change in total
-- scaled_balance must equal that reserve-day's total diff_scaled. Both sides are Int256 aToken
-- WadRayMath and index-independent, so no price/index movement confounds the check. A residual
-- LARGE RELATIVE TO the reserve's supply means the carry-forward baked a value the source
-- deltas do not justify -- i.e. the seed was mis-counted. A legitimate large deposit day is
-- NOT flagged: its change is present in diff_scaled, so its residual is ~0.
--
-- Relative tolerance (default 10%), not exact equality: reading this ReplacingMergeTree without
-- FINAL, transient unmerged-duplicate parts contribute small merge-lag noise (observed <=1.2%
-- of supply on the 2026-06-18/20 incident-adjacent days). The seed-doubling residual is ~50%
-- of supply (a full 2x adds ~100% of the prior level), so 10% cleanly separates signal from
-- merge-lag dust with a ~5x margin on each side. Tune via var lending_scaled_conservation_rel_tol.
--
-- This is the exact detector for the 2026-06-19 / 2026-06-21 seed-doubling that inflated every
-- Aave V3 + SparkLend reserve ~4x (reported lending TVL ~$171M vs true ~$43M) and rode the
-- cumulative carry-forward forward for a month. The pre-fix densify+UNION-ALL seed read
-- unmerged ReplacingMergeTree duplicate parts on the seed day twice, doubling every holder's
-- balance with no matching diff -- exactly what this HAVING clause trips on.
-- Lessons: refill-append-aggregator-inflation, sparse-zero-row-stale-survival.
--
-- Steady-state caveat: this is a STEP check -- it fires on the day an inflation is introduced,
-- not on an already-baked flat plateau. Pair it with a periodic reconciliation of
-- sum(scaled_balance) per reserve to on-chain aToken.scaledTotalSupply()
-- (aToken = lending_market_mapping.supply_token_address) to catch a persistent offset.
WITH bal AS (
    SELECT
        date,
        protocol,
        reserve_address,
        sum(scaled_balance) AS scaled_sum
    FROM `dbt`.`int_execution_lending_aave_user_balances_daily`
    -- one extra day so the earliest evaluated day has a prior day to diff against
    WHERE date >= today() - 7 - 1
    GROUP BY date, protocol, reserve_address
),

diffs AS (
    SELECT
        date,
        protocol,
        reserve_address,
        sum(diff_scaled) AS diff_sum
    FROM `dbt`.`int_execution_lending_aave_diffs_daily`
    WHERE date >= today() - 7 - 1
    GROUP BY date, protocol, reserve_address
),

joined AS (
    SELECT
        b.date,
        b.protocol,
        b.reserve_address,
        b.scaled_sum,
        b.scaled_sum - lagInFrame(b.scaled_sum) OVER (
            PARTITION BY b.protocol, b.reserve_address
            ORDER BY b.date
        ) AS observed_delta,
        coalesce(d.diff_sum, toInt256(0)) AS expected_delta
    FROM bal b
    LEFT JOIN diffs d
        ON  d.date            = b.date
        AND d.protocol        = b.protocol
        AND d.reserve_address = b.reserve_address
),

evaluated AS (
    SELECT
        date,
        protocol,
        reserve_address,
        scaled_sum,
        observed_delta,
        expected_delta,
        observed_delta - expected_delta AS residual_scaled,
        abs(toFloat64(observed_delta - expected_delta)) / nullIf(abs(toFloat64(scaled_sum)), 0) AS rel_residual
    FROM joined
)

SELECT
    date,
    protocol,
    reserve_address,
    scaled_sum,
    observed_delta,
    expected_delta,
    residual_scaled,
    rel_residual
FROM evaluated
-- drop the seed day (its lag is out-of-window / 0 and would false-positive)
WHERE date >= today() - 7
  AND rel_residual > 0.1
ORDER BY rel_residual DESC