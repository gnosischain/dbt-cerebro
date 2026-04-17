{{
    config(
        materialized='view',
        tags=['dev', 'live', 'execution', 'pools', 'trades', 'api']
    )
}}

SELECT
    max(block_timestamp)                                    AS newest_block_timestamp,
    now()                                                   AS server_now,
    dateDiff('second', max(block_timestamp), now())         AS lag_seconds
FROM {{ source('execution_live', 'logs') }}
