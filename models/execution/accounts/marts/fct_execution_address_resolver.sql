{{
    config(
        materialized='table',
        engine='AggregatingMergeTree()',
        order_by='(address)',
        unique_key='address',
        settings={'allow_nullable_key': 1},
        pre_hook=[
          "SET max_threads = 1",
          "SET max_block_size = 8192",
          "SET max_memory_usage = 10000000000",
          "SET max_bytes_before_external_sort = 100000000"
        ],
        post_hook=[
          "SET max_threads = 0",
          "SET max_block_size = 65505",
          "SET max_memory_usage = 0",
          "SET max_bytes_before_external_sort = 0"
        ],
        tags=['production', 'execution', 'accounts', 'fct:address_resolver', 'granularity:latest']
    )
}}

-- One row per (address × source) signal. The dashboard always queries this
-- table with a single-address filter (`api_execution_address_resolver` view
-- + `WHERE address = '0x…'`), so we push the cross-source merge to query
-- time instead of materialising a 5–10M-row outer GROUP BY at build time
-- (which OOMs the 10.8 GiB cluster cap during AggregatingTransform).
--
-- Engine is AggregatingMergeTree on `address` so background merges collapse
-- the per-source rows into a single row per address using the MAX of each
-- flag/count column. The api view does a `GROUP BY address` with
-- `maxMerge(...)` (or just `max(...)` since columns are already plain ints
-- here) on the cheap per-address point-lookup path.
--
-- Sources contributing rows (one tagged row per source per address):
--   1. Safe contract deployment (the safe itself)
--   2. Safe ownership (owner of some safe — also stamps connected_safe_count)
--   3. Circles v2 avatar registration (also stamps name + avatar_type)
--   4. Gnosis Pay wallet lifetime metrics
--   5. Consensus validator status snapshot as a withdrawal_address
--
-- Validator withdrawal addresses are derived inline from withdrawal_credentials
-- (type 0x01 / 0x02) so we don't depend on a possibly-stale upstream column.

WITH
  safes AS (
    SELECT DISTINCT lower(safe_address) AS address
    FROM {{ ref('int_execution_safes') }}
    WHERE safe_address IS NOT NULL
  ),
  safe_owners AS (
    -- HyperLogLog uniq() instead of count(DISTINCT) — keeps state ~64 KB
    -- per group instead of holding the full safe_address set in RAM.
    SELECT
      lower(owner) AS address,
      toUInt64(uniq(safe_address)) AS connected_safe_count
    FROM {{ ref('int_execution_safes_current_owners') }}
    WHERE owner IS NOT NULL
      AND safe_address IS NOT NULL
    GROUP BY address
  ),
  circles AS (
    SELECT
      lower(avatar) AS address,
      any(name) AS circles_name,
      any(avatar_type) AS circles_avatar_type
    FROM {{ ref('int_execution_circles_v2_avatars') }}
    WHERE avatar IS NOT NULL
    GROUP BY address
  ),
  gpay AS (
    SELECT DISTINCT lower(wallet_address) AS address
    FROM {{ ref('fct_execution_gpay_user_lifetime_metrics') }}
    WHERE wallet_address IS NOT NULL
  ),
  validator_wd AS (
    SELECT
      address,
      count() AS connected_validator_count
    FROM (
      SELECT
        CASE
          WHEN startsWith(withdrawal_credentials, '0x01') OR startsWith(withdrawal_credentials, '0x02')
            THEN concat('0x', lower(substring(withdrawal_credentials, 27, 40)))
          ELSE NULL
        END AS address
      FROM {{ ref('fct_consensus_validators_status_latest') }}
      WHERE withdrawal_credentials IS NOT NULL
    )
    WHERE address IS NOT NULL
    GROUP BY address
  )

SELECT address, 1 AS is_safe, 0 AS is_safe_owner, 0 AS is_circles_avatar,
       0 AS is_gpay_wallet, 0 AS is_validator_withdrawal_address,
       toUInt64(0) AS connected_safe_count,
       toUInt64(0) AS connected_validator_count,
       CAST(NULL AS Nullable(String)) AS circles_name,
       CAST(NULL AS Nullable(String)) AS circles_avatar_type
FROM safes
WHERE address != ''
UNION ALL
SELECT address, 0, 1, 0, 0, 0,
       connected_safe_count,
       toUInt64(0),
       CAST(NULL AS Nullable(String)),
       CAST(NULL AS Nullable(String))
FROM safe_owners
WHERE address != ''
UNION ALL
SELECT address, 0, 0, 1, 0, 0,
       toUInt64(0),
       toUInt64(0),
       CAST(nullIf(circles_name, '') AS Nullable(String)),
       CAST(nullIf(circles_avatar_type, '') AS Nullable(String))
FROM circles
WHERE address != ''
UNION ALL
SELECT address, 0, 0, 0, 1, 0,
       toUInt64(0),
       toUInt64(0),
       CAST(NULL AS Nullable(String)),
       CAST(NULL AS Nullable(String))
FROM gpay
WHERE address != ''
UNION ALL
SELECT address, 0, 0, 0, 0, 1,
       toUInt64(0),
       connected_validator_count,
       CAST(NULL AS Nullable(String)),
       CAST(NULL AS Nullable(String))
FROM validator_wd
WHERE address != ''
