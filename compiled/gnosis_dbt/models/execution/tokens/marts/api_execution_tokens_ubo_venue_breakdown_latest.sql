

SELECT sub.*, (SELECT toDate(max(date)) FROM `dbt`.`int_rpc_state_indexer_token_balances_priced_daily`) AS as_of_date
FROM (
SELECT token_address, symbol, token_class, venue, balance, balance_usd, percentage
FROM `dbt`.`fct_execution_tokens_ubo_venue_breakdown_latest`
ORDER BY token_address, balance_usd DESC
) AS sub