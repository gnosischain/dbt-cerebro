-- Per-day population coverage of the income fact vs the validator snapshots.
--
-- int_consensus_validators_income_daily builds in 6 validator_index bands
-- (meta.full_refresh stages, batch_days=1) driven by the microbatch runner. When
-- some bands fail or are skipped, the day is left holding a single band (~100k of
-- ~558k validators) and every downstream aggregate silently averages a biased
-- cohort: 2026-07-08..25 ran with 1 of 6 bands on most days, which pushed the
-- latest-day avg_apy in int_consensus_validators_dists_daily to -0.91 (surviving
-- band was a mostly-exited cohort whose stragglers bleed inactivity penalties)
-- and pinned 07-20 at the spec-cap APY ~35.7 for 56k validators. The spec-cap /
-- ledger-identity tests all pass on such days — only a population check sees it.
--
-- Grain check, not value check: income emits one row per (date, validator_index)
-- for every validator in the snapshot (including exited/zero-balance), so daily
-- unique-validator counts must match near-exactly. uniqExact on both sides keeps
-- the comparison robust to unmerged RMT duplicates.
--
-- Returns offending days; passing = zero rows. Yesterday is included because the
-- cron builds the income model before tests run.
SELECT
    s.date AS d
    ,s.snapshot_validators AS snapshot_validators
    ,coalesce(i.income_validators, 0) AS income_validators
    ,round(coalesce(i.income_validators, 0) / s.snapshot_validators, 4) AS coverage
FROM (
    SELECT date, uniqExact(validator_index) AS snapshot_validators
    FROM `dbt`.`int_consensus_validators_snapshots_daily`
    WHERE date >= today() - 7
      AND date < today()
    GROUP BY date
) s
LEFT JOIN (
    SELECT date, uniqExact(validator_index) AS income_validators
    FROM `dbt`.`int_consensus_validators_income_daily`
    WHERE date >= today() - 7
      AND date < today()
    GROUP BY date
) i ON i.date = s.date
WHERE coalesce(i.income_validators, 0) < 0.99 * s.snapshot_validators