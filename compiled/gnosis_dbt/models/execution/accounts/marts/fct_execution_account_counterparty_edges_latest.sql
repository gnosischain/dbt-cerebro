

WITH daily_edges AS (
  SELECT
    source,
    target,
    edge_type,
    sum(weight) AS weight,
    sum(raw_volume) AS raw_volume,
    max(last_seen_date) AS last_seen_date
  FROM `dbt`.`fct_execution_account_counterparty_edges_daily`
  GROUP BY source, target, edge_type
),

safe_edges AS (
  SELECT
    lower(owner) AS source,
    lower(safe_address) AS target,
    'safe_relation' AS edge_type,
    toUInt64(1) AS weight,
    toUInt256(0) AS raw_volume,
    toDate(max(became_owner_at)) AS last_seen_date
  FROM `dbt`.`int_execution_safes_current_owners`
  WHERE owner IS NOT NULL
    AND safe_address IS NOT NULL
  GROUP BY source, target, edge_type

  UNION ALL

  SELECT
    lower(safe_address) AS source,
    lower(owner) AS target,
    'safe_relation' AS edge_type,
    toUInt64(1) AS weight,
    toUInt256(0) AS raw_volume,
    toDate(max(became_owner_at)) AS last_seen_date
  FROM `dbt`.`int_execution_safes_current_owners`
  WHERE owner IS NOT NULL
    AND safe_address IS NOT NULL
  GROUP BY source, target, edge_type
),

circles_trust_edges AS (
  SELECT truster AS avatar, trustee AS counterparty, valid_from
  FROM `dbt`.`fct_execution_circles_v2_trust_relations_current`
  WHERE truster IS NOT NULL

  UNION ALL

  SELECT trustee AS avatar, truster AS counterparty, valid_from
  FROM `dbt`.`fct_execution_circles_v2_trust_relations_current`
  WHERE trustee IS NOT NULL
),

circles_edges AS (
  SELECT
    lower(avatar) AS source,
    lower(counterparty) AS target,
    'circles_trust' AS edge_type,
    toUInt64(1) AS weight,
    toUInt256(0) AS raw_volume,
    toDate(max(valid_from)) AS last_seen_date
  FROM circles_trust_edges
  WHERE avatar IS NOT NULL
    AND counterparty IS NOT NULL
  GROUP BY source, target, edge_type
),

validator_edges AS (
  SELECT
    lower(withdrawal_address) AS source,
    withdrawal_credentials AS target,
    'validator_relation' AS edge_type,
    count() AS weight,
    toUInt256(0) AS raw_volume,
    toDate(max(slot_timestamp)) AS last_seen_date
  FROM `dbt`.`fct_consensus_validators_status_latest`
  WHERE withdrawal_address IS NOT NULL
    AND withdrawal_credentials IS NOT NULL
  GROUP BY source, target, edge_type
)

SELECT * FROM daily_edges
UNION ALL
SELECT * FROM safe_edges
UNION ALL
SELECT * FROM circles_edges
UNION ALL
SELECT * FROM validator_edges