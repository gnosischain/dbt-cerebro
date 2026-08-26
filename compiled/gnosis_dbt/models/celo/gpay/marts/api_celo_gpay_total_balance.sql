

-- Spendable stablecoin float held across all Celo GP card Safes (latest day),
-- mark-to-market in USD. Scope is token_class = 'STABLECOIN' (USDT/USDC in practice);
-- RWA reward holdings are the separate RewardBalance label, not included here.
SELECT sub.*, (SELECT toDate(max(date)) FROM `dbt`.`int_celo_gpay_activity_daily`) AS as_of_date
FROM (
SELECT value
FROM `dbt`.`fct_celo_gpay_snapshots`
WHERE label = 'TotalBalance' AND window = 'All'
) AS sub