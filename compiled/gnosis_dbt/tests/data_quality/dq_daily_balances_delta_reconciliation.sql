
-- Frontier-day integration check: every diff the diffs model holds for a day must be
-- reflected in the balances model's day-over-day movement for that (token, address).
-- The balances chain is cumulative and never revisits a built day, so when a daily
-- slice is built while the diffs table is still partial (server saturation, late
-- upstream), the diffs self-heal but the balances freeze the hole forever — visible
-- only as a slow burst of negative/stale balances days later. Lesson:
-- frontier-day-incomplete-inputs — 2026-07-15 (201 negatives) and 2026-08-23
-- (4,370 of 4,582 deltas unapplied, 135 new negatives) were both invisible to every
-- other test the morning after; this exact query scoped both.
--
-- Join semantics: no join_use_nulls here (tests carry no hooks), so unmatched rows
-- read as 0 — a missing balance row is flagged through the same arithmetic
-- (0 - prev != delta) unless the expected balance is genuinely zero, which the
-- sparse-zero-row-stale-survival test owns. Dates beyond the balances watermark are
-- excluded: a day the balances model has not built at all is a plain run failure
-- (Elementary shows it), not a frozen hole.
WITH d AS (
    SELECT
        date,
        token_address,
        address,
        any(toInt256(net_delta_raw)) AS delta,
        date - 1                     AS prev_date
    FROM `dbt`.`int_execution_tokens_address_diffs_daily`
    WHERE date >= today() - 3
      AND date <= (SELECT max(date) FROM `dbt`.`int_execution_tokens_balances_native_daily`)
    GROUP BY date, token_address, address
),

b AS (
    SELECT
        date,
        token_address,
        address,
        any(toInt256(balance_raw)) AS bal
    FROM `dbt`.`int_execution_tokens_balances_native_daily`
    WHERE date >= today() - 4
    GROUP BY date, token_address, address
)

SELECT
    d.date,
    countIf((t.bal - p.bal) != d.delta) AS deltas_not_applied
FROM d
LEFT JOIN b AS t
    ON t.date = d.date AND t.token_address = d.token_address AND t.address = d.address
LEFT JOIN b AS p
    ON p.date = d.prev_date AND p.token_address = d.token_address AND p.address = d.address
GROUP BY d.date
HAVING deltas_not_applied > 0
ORDER BY d.date