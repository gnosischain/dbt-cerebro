{{
  config(
    materialized='table',
    engine='ReplacingMergeTree()',
    order_by='(root_address, relation, entity_id)',
    unique_key='(root_address, relation, entity_id)',
    settings={ 'allow_nullable_key': 1 },
    pre_hook=[
      "SET max_threads = 1",
      "SET max_block_size = 8192",
      "SET max_memory_usage = 10000000000",
      "SET max_bytes_before_external_group_by = 100000000",
      "SET max_bytes_before_external_sort = 100000000",
      "SET group_by_two_level_threshold = 10000",
      "SET group_by_two_level_threshold_bytes = 10000000"
    ],
    post_hook=[
      "SET max_threads = 0",
      "SET max_block_size = 65505",
      "SET max_memory_usage = 0",
      "SET max_bytes_before_external_group_by = 0",
      "SET max_bytes_before_external_sort = 0"
    ],
    tags=['production', 'execution', 'accounts', 'portfolio', 'linked_entities', 'granularity:latest']
  )
}}

WITH safe_owner_of AS (
  SELECT
    lower(owner) AS root_address,
    'safe' AS entity_type,
    lower(safe_address) AS entity_id,
    lower(safe_address) AS entity_address,
    'safe_owner_of' AS relation,
    concat('Safe ', substring(lower(safe_address), 1, 10), '...', substring(lower(safe_address), length(lower(safe_address)) - 5, 6)) AS display_label,
    toUInt64(1) AS value_count,
    max(became_owner_at) AS last_seen_at
  FROM {{ ref('int_execution_safes_current_owners') }}
  WHERE owner IS NOT NULL
    AND safe_address IS NOT NULL
  GROUP BY root_address, entity_type, entity_id, entity_address, relation, display_label
),

safe_owned_by AS (
  SELECT
    lower(safe_address) AS root_address,
    'safe_owner' AS entity_type,
    lower(owner) AS entity_id,
    lower(owner) AS entity_address,
    'safe_owned_by' AS relation,
    concat('Owner ', substring(lower(owner), 1, 10), '...', substring(lower(owner), length(lower(owner)) - 5, 6)) AS display_label,
    toUInt64(1) AS value_count,
    max(became_owner_at) AS last_seen_at
  FROM {{ ref('int_execution_safes_current_owners') }}
  WHERE owner IS NOT NULL
    AND safe_address IS NOT NULL
  GROUP BY root_address, entity_type, entity_id, entity_address, relation, display_label
),

gpay_controlled AS (
  SELECT
    lower(first_ga_owner_address) AS root_address,
    'gpay_wallet' AS entity_type,
    lower(pay_wallet) AS entity_id,
    lower(pay_wallet) AS entity_address,
    'gnosis_app_controls_gpay_wallet' AS relation,
    concat('Gnosis Pay wallet ', substring(lower(pay_wallet), 1, 10), '...', substring(lower(pay_wallet), length(lower(pay_wallet)) - 5, 6)) AS display_label,
    toUInt64(n_ga_owners_current) AS value_count,
    last_event_at AS last_seen_at
  FROM {{ ref('int_execution_gnosis_app_gpay_wallets') }}
  WHERE first_ga_owner_address IS NOT NULL
    AND pay_wallet IS NOT NULL
    AND is_currently_ga_owned = 1
),

validator_credentials AS (
  SELECT
    derived_withdrawal_address AS root_address,
    'validator_credential' AS entity_type,
    withdrawal_credentials AS entity_id,
    derived_withdrawal_address AS entity_address,
    'validator_withdrawal_credential' AS relation,
    concat(toString(count()), ' validators · ', substring(withdrawal_credentials, 1, 10), '...', substring(withdrawal_credentials, length(withdrawal_credentials) - 5, 6)) AS display_label,
    count() AS value_count,
    max(slot_timestamp) AS last_seen_at
  FROM (
    SELECT
      withdrawal_credentials,
      slot_timestamp,
      -- Inline derivation: handles upstream tables that don't yet expose a
      -- materialized `withdrawal_address` column. Type 0x01/0x02 credentials
      -- pack the address in the last 20 bytes (hex chars 27..66).
      CASE
        WHEN startsWith(withdrawal_credentials, '0x01') OR startsWith(withdrawal_credentials, '0x02')
          THEN concat('0x', lower(substring(withdrawal_credentials, 27, 40)))
        ELSE NULL
      END AS derived_withdrawal_address
    FROM {{ ref('fct_consensus_validators_status_latest') }}
    WHERE withdrawal_credentials IS NOT NULL
  )
  WHERE derived_withdrawal_address IS NOT NULL
  GROUP BY root_address, entity_type, entity_id, entity_address, relation
)

SELECT * FROM safe_owner_of
UNION ALL
SELECT * FROM safe_owned_by
UNION ALL
SELECT * FROM gpay_controlled
UNION ALL
SELECT * FROM validator_credentials

