

SELECT
    type,
    token,
    name,
    address,
    yield_pct,
    yield_label,
    borrow_apy,
    tvl,
    total_supplied,
    total_borrowed,
    fees_7d,
    lvr_apr_7d,
    net_apr_7d,
    utilization_rate,
    protocol,
    fee_pct
FROM `dbt`.`fct_execution_yields_opportunities_latest`
ORDER BY yield_pct DESC