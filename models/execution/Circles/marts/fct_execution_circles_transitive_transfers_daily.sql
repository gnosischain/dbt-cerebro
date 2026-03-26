{{ 
    config(
        materialized='view',
        tags=['production','execution','circles','transitive_transfers']
    )
}}

SELECT
  toStartOfDay(block_timestamp) AS date,
  decoded_params['from'] AS from_avatar,
  decoded_params['to'] AS to_avatar,
  SUM(
    toUInt256OrZero(
      arrayJoin(
        JSONExtract(
          ifNull(decoded_params['amounts'], '[]'),
          'Array(String)'
        )
      )
    )
  ) AS total_amount,
  COUNT(*) AS cnt
FROM {{ ref('contracts_circles_v2_Hub_events') }}
WHERE event_name = 'StreamCompleted'
GROUP BY 1, 2, 3
