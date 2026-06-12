

-- Average trades settled per batch per day.
-- On Gnosis Chain, CoW peer-to-peer matching is rare (~1% of batches
-- historically), so nearly all batches contain a single trade routed
-- through external DEX liquidity. Values > 1 indicate days where
-- the solver matched multiple orders internally.
SELECT
    date,
    round(num_trades / nullIf(num_batches, 0), 2)                                AS value
FROM `dbt`.`fct_execution_cow_daily`
WHERE date < today()
  AND num_batches > 0
ORDER BY date