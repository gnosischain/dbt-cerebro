{{ 
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        engine='ReplacingMergeTree()',
        order_by='(date)',
        unique_key='(date)',
        partition_by='toStartOfMonth(date)',
        tags=['production','execution','yields','sdai_rate']
    )
}}


WITH 

sdai_rate_sparse_daily AS (
    SELECT
        toStartOfDay(block_timestamp) AS date
        ,argMin(
          toUInt256OrNull(decoded_params['assets']) / toUInt256OrNull(decoded_params['shares']),
          block_timestamp
        ) AS sdai_conversion
    FROM 
        {{ ref('contracts_sdai_events') }}
    WHERE 
        event_name = 'Deposit'
        AND toUInt256OrNull(decoded_params['shares']) != 0
        AND block_timestamp < today()
        {{ apply_monthly_incremental_filter('block_timestamp','date','true') }}
    GROUP BY 1
),

calendar AS (
    SELECT
        arrayJoin(
            arrayMap(
                x -> toStartOfDay(start_date + x),
                range(toUInt32(end_date - start_date) + 1)
            )
        ) AS date
    FROM (
        SELECT 
          min(toDate(date)) AS start_date
          ,max(toDate(date)) AS end_date
        FROM sdai_rate_sparse_daily
    )
),

{% if is_incremental() %}
last_partition_value AS (
    SELECT 
        sdai_conversion
    FROM 
        {{ this }}
    WHERE
        toStartOfMonth(date) = (
            SELECT addMonths(max(toStartOfMonth(date)), -1)
            FROM {{ this }}
        )
    ORDER BY date DESC
    LIMIT 1
),
{% endif %}

sdai_daily_rate AS (
  SELECT
      date
      ,sdai_conversion
      ,floor(
          sdai_conversion 
          - (
            {% if is_incremental() %}
            COALESCE(
                lagInFrame(sdai_conversion) OVER (
                    ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                ),
                (SELECT sdai_conversion FROM last_partition_value)
            )
            {% else %}
            lagInFrame(sdai_conversion) OVER (
                    ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                )
            {% endif %}
            )
      ,12) AS rate
  FROM (
    SELECT 
      t1.date
      ,last_value(t2.sdai_conversion) ignore nulls OVER (ORDER BY t1.date) AS sdai_conversion
    FROM calendar t1
    LEFT JOIN
      sdai_rate_sparse_daily t2
      ON t2.date = t1.date
  )
)


SELECT 
  date
  ,sdai_conversion
  ,rate
FROM sdai_daily_rate
WHERE rate IS NOT NULL 