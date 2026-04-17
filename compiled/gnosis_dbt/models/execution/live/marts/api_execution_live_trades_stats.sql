

SELECT
    count()                                                                         AS trade_count,
    round(sum(trade_usd), 0)                                                        AS volume_usd,
    uniqExact(trader)                                                               AS unique_traders,
    round(100.0 * countIf(aggregator IS NOT NULL) / nullIf(count(), 0), 1)          AS aggregator_share_pct,
    round(100.0 * countIf(hops > 1)               / nullIf(count(), 0), 1)          AS multihop_share_pct
FROM `dbt`.`api_execution_live_trades`