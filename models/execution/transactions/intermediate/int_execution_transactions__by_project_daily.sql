{{
  config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    engine='ReplacingMergeTree()',
    order_by='(day, project)',
    unique_key='(day, project)',
    partition_by='toStartOfMonth(day)',
    settings={'allow_nullable_key': 1},
    tags=['production','execution','transactions']
  )
}}

WITH tx AS (
  SELECT
    block_timestamp,
    toDate(block_timestamp)            AS day,
    transaction_hash,
    from_address,
    to_address,
    input,
    toFloat64(gas_used)   AS gas_used,
    toFloat64(gas_price)  AS gas_price
  FROM {{ ref('stg_execution__transactions') }}
  {% if is_incremental() %}
    WHERE block_timestamp >= date_trunc('month', now() - INTERVAL 35 DAY)
  {% endif %}
),
lbl AS (
  SELECT address, project FROM {{ ref('stg_execution_transactions__labels') }}
),
classified AS (
  SELECT
    t.day,
    multiIf(
      l.project IS NOT NULL, l.project,
      (t.input = '' OR t.input = '0x' OR t.input IS NULL), 'EOA',
      'Others'
    )                                    AS project,
    COUNT()                               AS tx_count,
    countDistinct(t.from_address)         AS active_accounts,
    SUM(t.gas_used * t.gas_price) / 1e18  AS fee_native_sum
  FROM tx t
  LEFT JOIN lbl l ON t.to_address = l.address
  GROUP BY t.day, project
),
px AS (
  SELECT price_date, anyLast(price_usd) AS price_usd
  FROM {{ ref('stg_execution_transactions__prices') }}
  GROUP BY price_date
)
SELECT
  c.day,
  c.project,
  c.tx_count,
  c.active_accounts,
  c.fee_native_sum,
  c.fee_native_sum * COALESCE(px.price_usd, 1.0) AS fee_usd_sum
FROM classified c
LEFT JOIN px
  ON px.price_date = c.day