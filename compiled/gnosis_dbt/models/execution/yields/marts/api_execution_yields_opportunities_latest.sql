

SELECT
    type,
    token,
    name,
    address,
    pool_key,
    rate_trend_14d,
    yield_apr,
    yield_apy,
    borrow_apy,
    tvl,
    total_supplied,
    total_borrowed,
    fees_7d,
    volume_usd_7d,
    net_apr_7d,
    utilization_rate,
    protocol,
    fee_pct
FROM `dbt`.`fct_execution_yields_opportunities_latest`
ORDER BY COALESCE(yield_apr, yield_apy) DESC