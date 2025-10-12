{{ 
  config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    engine='ReplacingMergeTree()',
    order_by='(date, project)',
    unique_key='(date, project)',
    partition_by='toStartOfMonth(date)',
    settings={ 'allow_nullable_key': 1 },
    tags=['production','execution','transactions']
  ) 
}}

{% set month       = var('month', none) %}
{% set start_month = var('start_month', none) %}
{% set end_month   = var('end_month', none) %}

WITH lbl AS (
  SELECT address, project, sector
  FROM {{ ref('int_crawlers_data_labels') }}
),

tx_labeled AS (
  SELECT
    toDate(t.block_timestamp)                        AS date,
    coalesce(nullIf(trim(l.project), ''), 'Unknown') AS project,
    lower(t.from_address)                            AS from_address,
    toFloat64(coalesce(t.gas_used, 0))               AS gas_used,
    toFloat64(coalesce(t.gas_price, 0))              AS gas_price
  FROM {{ ref('stg_execution__transactions') }} t
  ANY LEFT JOIN lbl l ON lower(t.to_address) = l.address
  WHERE t.block_timestamp < today()
    AND t.from_address IS NOT NULL
    AND t.success = 1
    {% if var('start_month', none) and var('end_month', none) %}
      AND toStartOfMonth(t.block_timestamp) >= toDate('{{ var("start_month") }}')
      AND toStartOfMonth(t.block_timestamp) <= toDate('{{ var("end_month") }}')
    {% endif %}
    {{ apply_monthly_incremental_filter('block_timestamp', 'date', 'true') }}
),

agg AS (
  SELECT
    date,
    project,
    count()                                    AS tx_count,
    groupBitmapState(cityHash64(from_address)) AS ua_bitmap_state,
    sum(gas_used)                              AS gas_used_sum,
    sum(gas_used * gas_price) / 1e18           AS fee_native_sum
  FROM tx_labeled
  GROUP BY date, project
),

proj_sector AS (
  SELECT
    project,
    coalesce(nullIf(trim(sector), ''), 'Unknown') AS sector
  FROM (
    SELECT project, anyHeavy(sector) AS sector
    FROM {{ ref('int_crawlers_data_labels') }}
    GROUP BY project
  )
)

SELECT
  a.date                AS date,
  a.project             AS project,
  ps.sector             AS sector,
  a.tx_count            AS tx_count,
  a.ua_bitmap_state     AS ua_bitmap_state,
  a.gas_used_sum        AS gas_used_sum,
  a.fee_native_sum      AS fee_native_sum
FROM agg a
LEFT JOIN proj_sector ps ON ps.project = a.project
