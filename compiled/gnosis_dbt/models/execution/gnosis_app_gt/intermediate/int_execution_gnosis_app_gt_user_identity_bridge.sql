

-- Internal bridge: registered-identity address -> pseudonym. Uses the shared
-- CEREBRO_PII_SALT (pseudonymize_address), so user_pseudonym is joinable with
-- mixpanel user_id_hash / gpay / circles pseudonyms. Raw address present ->
-- internal-only. A pseudonym-only public view is exposed downstream.
SELECT
    address,
    
    sipHash64(concat(unhex('00'), lower(address)))
   AS user_pseudonym
FROM `dbt`.`int_execution_gnosis_app_gt_user_dim`