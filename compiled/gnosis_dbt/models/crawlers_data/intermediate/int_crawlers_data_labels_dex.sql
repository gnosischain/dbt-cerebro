

SELECT
  address,
  project
FROM `dbt`.`int_crawlers_data_labels`
WHERE sector NOT IN ('EOAs', 'ERC20 Tokens', 'Wallets & AA', 'Payments')