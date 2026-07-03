

SELECT sub.*, (SELECT toDate(max(block_timestamp)) FROM `dbt`.`int_execution_safes_module_events`) AS as_of_date
FROM (
SELECT
    -- Count distinct CANONICAL cards among GA-owned safes: a migrated pair (old + inherited new)
    -- is one card, so collapse old->new to avoid double-counting (see gpay_wallets_daily).
    uniqExactIf(if(c.canonical_address != '', c.canonical_address, w.pay_wallet),
                w.is_currently_ga_owned)              AS value,
    CAST(NULL AS Nullable(Float64))                   AS change_pct
FROM `dbt`.`int_execution_gnosis_app_gpay_wallets` w
LEFT JOIN `dbt`.`int_execution_gpay_safe_canonical` c ON c.address = w.pay_wallet
) AS sub