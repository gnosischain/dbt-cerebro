{{ 
    config(
        materialized            = 'incremental',
        incremental_strategy    = 'delete+insert',
        engine                  = 'ReplacingMergeTree()',
        order_by                = '(date, from_avatar, to_avatar)',
        unique_key              = '(date, from_avatar, to_avatar)',
        partition_by            = 'toStartOfMonth(date)',
        settings                = { 
                                    'allow_nullable_key': 1 
                                },
    )
}}


SELECT
  toStartOfDay(block_timestamp) AS date,
  decoded_params['from'] AS from_avatar,
  decoded_params['to']   AS  to_avatar,
  SUM(
    toUInt256OrZero(
      arrayJoin(
        JSONExtract(
          ifNull(decoded_params['amounts'], '[]'),   -- remove Nullable
          'Array(String)'                            -- get Array(String)
        )
      )
    )
  ) AS total_amount
  ,COUNT(*) AS cnt
FROM {{ ref('contracts_circles_v2_Hub_events') }}
WHERE
  event_name = 'StreamCompleted'
  {{ apply_monthly_incremental_filter(source_field='block_timestamp', destination_field='date', add_and=true) }}
GROUP BY 1, 2, 3