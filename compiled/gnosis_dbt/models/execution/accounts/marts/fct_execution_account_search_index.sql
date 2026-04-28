

WITH profile AS (
  SELECT
    address,
    display_name,
    circles_name,
    is_safe,
    is_safe_owner,
    is_circles_avatar,
    is_gpay_wallet,
    is_gnosis_app_user,
    is_validator_withdrawal_address,
    is_lp_provider,
    is_lending_user,
    connected_safe_count,
    connected_validator_count
  FROM `dbt`.`fct_execution_account_profile_latest`
  WHERE address IS NOT NULL
),

profile_rows AS (
  SELECT
    address AS search_key,
    multiIf(is_safe, 'safe', is_gpay_wallet, 'gpay_wallet', is_gnosis_app_user, 'gnosis_app_user', 'address') AS result_type,
    address,
    coalesce(nullIf(display_name, ''), address) AS display_label,
    concat(
      if(is_circles_avatar, 'Circles avatar · ', ''),
      if(is_safe, 'Safe · ', ''),
      if(is_safe_owner, concat('Safe owner · ', toString(connected_safe_count), ' safes · '), ''),
      if(is_gpay_wallet, 'Gnosis Pay · ', ''),
      if(is_gnosis_app_user, 'Gnosis App · ', ''),
      if(is_validator_withdrawal_address, concat('Validator withdrawal address · ', toString(connected_validator_count), ' validators · '), ''),
      if(is_lp_provider OR is_lending_user, 'Yield activity · ', ''),
      address
    ) AS subtitle,
    concat(
      if(is_circles_avatar, 'Circles,', ''),
      if(is_safe, 'Safe,', ''),
      if(is_safe_owner, 'Safe owner,', ''),
      if(is_gpay_wallet, 'GPay,', ''),
      if(is_gnosis_app_user, 'Gnosis App,', ''),
      if(is_validator_withdrawal_address, 'Validators,', ''),
      if(is_lp_provider OR is_lending_user, 'Yield,', '')
    ) AS badges,
    CAST(NULL, 'Nullable(String)') AS validator_index,
    CAST(NULL, 'Nullable(String)') AS withdrawal_credentials,
    CAST(100 AS Int32) AS score_base
  FROM profile
),

circles_rows AS (
  SELECT
    lower(circles_name) AS search_key,
    'circles' AS result_type,
    address,
    circles_name AS display_label,
    concat('Circles avatar · ', address) AS subtitle,
    'Circles' AS badges,
    CAST(NULL, 'Nullable(String)') AS validator_index,
    CAST(NULL, 'Nullable(String)') AS withdrawal_credentials,
    CAST(300 AS Int32) AS score_base
  FROM profile
  WHERE circles_name IS NOT NULL
    AND circles_name != ''
),

validator_rows AS (
  SELECT
    toString(validator_index) AS search_key,
    'validator' AS result_type,
    lower(withdrawal_address) AS address,
    concat('Validator ', toString(validator_index)) AS display_label,
    concat('Withdrawal address · ', lower(withdrawal_address)) AS subtitle,
    'Validator' AS badges,
    toString(validator_index) AS validator_index,
    withdrawal_credentials,
    CAST(500 AS Int32) AS score_base
  FROM `dbt`.`fct_consensus_validators_explorer_members_table`
  WHERE withdrawal_address IS NOT NULL

  UNION ALL

  SELECT
    lower(pubkey) AS search_key,
    'validator' AS result_type,
    lower(withdrawal_address) AS address,
    concat('Validator ', toString(validator_index)) AS display_label,
    concat('Pubkey · ', lower(pubkey)) AS subtitle,
    'Validator' AS badges,
    toString(validator_index) AS validator_index,
    withdrawal_credentials,
    CAST(450 AS Int32) AS score_base
  FROM `dbt`.`fct_consensus_validators_explorer_members_table`
  WHERE withdrawal_address IS NOT NULL
    AND pubkey IS NOT NULL
),

credential_rows AS (
  SELECT
    lower(withdrawal_credentials) AS search_key,
    'validator_credential' AS result_type,
    lower(any(withdrawal_address)) AS address,
    concat('Withdrawal credential · ', toString(count()), ' validators') AS display_label,
    concat(substring(withdrawal_credentials, 1, 10), '...', substring(withdrawal_credentials, length(withdrawal_credentials) - 5, 6)) AS subtitle,
    'Validators' AS badges,
    toString(min(validator_index)) AS validator_index,
    withdrawal_credentials,
    CAST(480 AS Int32) AS score_base
  FROM `dbt`.`fct_consensus_validators_explorer_members_table`
  WHERE withdrawal_credentials IS NOT NULL
    AND withdrawal_address IS NOT NULL
  GROUP BY withdrawal_credentials
)

SELECT * FROM profile_rows
UNION ALL
SELECT * FROM circles_rows
UNION ALL
SELECT * FROM validator_rows
UNION ALL
SELECT * FROM credential_rows