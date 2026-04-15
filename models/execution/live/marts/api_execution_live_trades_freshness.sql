{{
    config(
        materialized='view',
        tags=['live', 'execution', 'pools', 'trades', 'api']
    )
}}

{#
    One-row freshness model for the live-trades tab. Renders a "Data as of X,
    lag Y min" indicator so users know whether the feed is actually live.
    Reads directly from `execution_live.logs` because this view must work
    even when the trades feed is empty (e.g. indexer just started).
#}

SELECT
    max(block_timestamp)                                    AS newest_block_timestamp,
    now()                                                   AS server_now,
    dateDiff('second', max(block_timestamp), now())         AS lag_seconds
FROM {{ source('execution_live', 'logs') }}
