

-- PUBLIC per-wallet metric rollup: pseudonym-only boundary over
-- int_execution_gnosis_app_gt_wallet_metrics (same CEREBRO_PII_SALT, joinable to
-- mixpanel/gpay/circles pseudonyms). Drops the raw address; carries the full
-- lifecycle / engagement / trust / segment metric surface.
SELECT
    
    sipHash64(concat(unhex('00'), lower(address)))
 AS user_pseudonym,
    * EXCEPT (address)
FROM `dbt`.`int_execution_gnosis_app_gt_wallet_metrics`