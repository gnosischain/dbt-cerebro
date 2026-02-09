

SELECT
    type,
    name,
    yield_pct,
    borrow_apy,
    tvl,
    protocol
FROM `dbt`.`fct_execution_yields_opportunities_latest`
ORDER BY yield_pct DESC