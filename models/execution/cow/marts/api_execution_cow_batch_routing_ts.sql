{{
  config(
    materialized='view',
    tags=['production', 'execution', 'cow', 'tier1',
          'api:cow_batch_routing_ts', 'granularity:daily']
  )
}}

SELECT
    date,
    label,
    round(n * 100.0 / sum(n) OVER (PARTITION BY date), 2) AS value
FROM (
    SELECT
        toDate(block_timestamp)                                                      AS date,
        multiIf(
            is_cow,
                'Pure CoW',
            num_trades > 1
                AND num_interactions > 0
                AND num_interactions < num_trades,
                'Partial CoW',
            'Pure DEX'
        )                                                                            AS label,
        count()                                                                      AS n
    FROM {{ ref('int_execution_cow_batches') }}
    WHERE toDate(block_timestamp) < today()
    GROUP BY date, label
)
ORDER BY date, label
