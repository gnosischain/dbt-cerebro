{{
    config(
        materialized='table',
        engine='MergeTree()',
        order_by=['truster', 'trustee'],
        settings={'allow_nullable_key': 1},
        tags=['production', 'execution', 'circles_v2', 'trusts']
    )
}}

WITH grouped AS (
    SELECT
        truster,
        trustee,
        arraySort(
            x -> (x.1, x.2, x.3),
            groupArray((
                block_timestamp,
                transaction_index,
                log_index,
                expiry_time
            ))
        ) AS events
    FROM {{ ref('int_execution_circles_v2_trust_updates') }}
    GROUP BY
        truster,
        trustee
),

prepared AS (
    SELECT
        truster,
        trustee,
        arrayMap(x -> x.1, events) AS start_agg,
        arrayMap(x -> x.4, events) AS expiry_agg,
        arrayMap(
            i -> if(
                i < length(events),
                events[i + 1].1,
                toDateTime('2106-02-07 06:28:15')
            ),
            arrayEnumerate(events)
        ) AS next_start_agg
    FROM grouped
),

intervals AS (
    SELECT
        truster,
        trustee,
        start_agg,
        arrayMap(
            (expiry_at, next_start_at) -> least(expiry_at, next_start_at),
            expiry_agg,
            next_start_agg
        ) AS end_agg
    FROM prepared
),

filtered AS (
    SELECT
        truster,
        trustee,
        arrayMap(x -> x.1, kept_ranges) AS valid_from_agg,
        arrayMap(x -> x.2, kept_ranges) AS valid_to_agg
    FROM (
        SELECT
            truster,
            trustee,
            arrayFilter(
                x -> x.2 > x.1,
                arrayZip(start_agg, end_agg)
            ) AS kept_ranges
        FROM intervals
    )
)

SELECT
    truster,
    trustee,
    valid_from_agg,
    valid_to_agg,
    length(valid_from_agg) AS range_count
FROM filtered
