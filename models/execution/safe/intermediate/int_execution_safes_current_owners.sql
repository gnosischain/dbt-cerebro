{{
  config(
    materialized='table',
    engine='ReplacingMergeTree()',
    order_by='(safe_address, owner)',
    settings={ 'allow_nullable_key': 1 },
    tags=['production','execution','safe'],
    pre_hook=[
        "SYSTEM DROP MARK CACHE",
        "SYSTEM DROP UNCOMPRESSED CACHE",
        "SYSTEM DROP COMPILED EXPRESSION CACHE",
        "SYSTEM DROP QUERY CACHE"
    ],
    query_settings={
        'max_threads': '1',
        'max_memory_usage': '2000000000',
        'memory_usage_overcommit_max_wait_microseconds': '60000000',
        'group_by_two_level_threshold': '1',
        'group_by_two_level_threshold_bytes': '1',
        'max_bytes_before_external_group_by': '20000000',
        'max_bytes_before_external_sort':     '20000000',
        'aggregation_memory_efficient_merge_threads': '1',
        'distributed_aggregation_memory_efficient': '1',
        'use_uncompressed_cache': '0',
        'use_query_cache': '0'
    }
  )
}}

-- Single scan of int_execution_safes_owner_events.
-- Owner-grain rows (with owner) and safe-grain threshold rows (no owner) are
-- aggregated in one GROUP BY (safe_address, owner) pass; threshold rows
-- naturally fall into their own bucket because owner IS NULL there.
-- We then compute current_threshold per safe_address as a window over the
-- aggregated rows — far smaller than re-scanning the source.
WITH base AS (
    SELECT
        safe_address,
        owner,
        event_kind,
        threshold,
        block_number,
        block_timestamp,
        log_index
    FROM {{ ref('int_execution_safes_owner_events') }}
    WHERE event_kind IN ('safe_setup','added_owner','removed_owner','changed_threshold')
),

agg AS (
    SELECT
        safe_address,
        owner,
        argMax(event_kind,      (block_number, log_index)) AS last_event_kind,
        argMax(block_timestamp, (block_number, log_index)) AS last_event_time,
        argMaxIf(threshold,     (block_number, log_index),
                 event_kind IN ('safe_setup','changed_threshold') AND threshold IS NOT NULL)
                                                            AS last_threshold_here,
        max((block_number, log_index))                       AS last_pos
    FROM base
    GROUP BY safe_address, owner
),

threshold_per_safe AS (
    SELECT
        safe_address,
        argMax(last_threshold_here, last_pos) AS latest_threshold
    FROM agg
    WHERE last_threshold_here IS NOT NULL
    GROUP BY safe_address
)

SELECT
    a.safe_address,
    a.owner,
    a.last_event_time           AS became_owner_at,
    t.latest_threshold          AS current_threshold
FROM agg a
LEFT JOIN threshold_per_safe t USING (safe_address)
WHERE a.owner IS NOT NULL
  AND a.last_event_kind IN ('safe_setup','added_owner')
