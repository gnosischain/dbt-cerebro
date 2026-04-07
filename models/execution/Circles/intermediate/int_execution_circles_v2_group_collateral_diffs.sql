{{
    config(
        materialized='incremental',
        incremental_strategy=('append' if var('start_month', none) else 'delete+insert'),
        engine='ReplacingMergeTree()',
        order_by='(block_timestamp, group_address, token_id, batch_index)',
        unique_key='(transaction_hash, log_index, batch_index, group_address, token_id, event_name)',
        partition_by='toStartOfMonth(block_timestamp)',
        settings={'allow_nullable_key': 1},
        tags=['production', 'execution', 'circles_v2', 'groups']
    )
}}

{% set start_month = var('start_month', none) %}
{% set end_month = var('end_month', none) %}
WITH single_locks AS (
    SELECT
        block_number,
        block_timestamp,
        lower(transaction_hash) AS transaction_hash,
        transaction_index,
        log_index,
        0 AS batch_index,
        event_name,
        lower(decoded_params['group']) AS group_address,
        toString(toUInt256OrZero(decoded_params['id'])) AS token_id,
        toInt256(toUInt256OrZero(decoded_params['value'])) AS delta_raw,
        CAST(NULL AS Nullable(String)) AS recipient
    FROM {{ ref('contracts_circles_v2_StandardTreasury_events') }}
    WHERE event_name = 'CollateralLockedSingle'
      {% if start_month and end_month %}
        AND toStartOfMonth(toDate(block_timestamp)) >= toDate('{{ start_month }}')
        AND toStartOfMonth(toDate(block_timestamp)) <= toDate('{{ end_month }}')
      {% else %}
        {{ apply_monthly_incremental_filter(source_field='block_timestamp', destination_field='block_timestamp', add_and=true) }}
      {% endif %}
),
batch_locks AS (
    SELECT
        block_number,
        block_timestamp,
        lower(transaction_hash) AS transaction_hash,
        transaction_index,
        log_index,
        batch_tuple.1 AS batch_index,
        event_name,
        lower(decoded_params['group']) AS group_address,
        batch_tuple.2.1 AS token_id,
        toInt256(toUInt256(batch_tuple.2.2)) AS delta_raw,
        CAST(NULL AS Nullable(String)) AS recipient
    FROM (
        SELECT
            block_number,
            block_timestamp,
            transaction_hash,
            transaction_index,
            log_index,
            event_name,
            decoded_params,
            arrayJoin(
                arrayZip(
                    arrayEnumerate(JSONExtract(coalesce(decoded_params['ids'], '[]'), 'Array(String)')),
                    arrayZip(
                        JSONExtract(coalesce(decoded_params['ids'], '[]'), 'Array(String)'),
                        JSONExtract(coalesce(decoded_params['values'], '[]'), 'Array(String)')
                    )
                )
            ) AS batch_tuple
        FROM {{ ref('contracts_circles_v2_StandardTreasury_events') }}
        WHERE event_name = 'CollateralLockedBatch'
          {% if start_month and end_month %}
            AND toStartOfMonth(toDate(block_timestamp)) >= toDate('{{ start_month }}')
            AND toStartOfMonth(toDate(block_timestamp)) <= toDate('{{ end_month }}')
          {% else %}
            {{ apply_monthly_incremental_filter(source_field='block_timestamp', destination_field='block_timestamp', add_and=true) }}
          {% endif %}
    )
),
batch_burns AS (
    SELECT
        block_number,
        block_timestamp,
        lower(transaction_hash) AS transaction_hash,
        transaction_index,
        log_index,
        batch_tuple.1 AS batch_index,
        event_name,
        lower(decoded_params['group']) AS group_address,
        batch_tuple.2.1 AS token_id,
        -toInt256(toUInt256(batch_tuple.2.2)) AS delta_raw,
        CAST(NULL AS Nullable(String)) AS recipient
    FROM (
        SELECT
            block_number,
            block_timestamp,
            transaction_hash,
            transaction_index,
            log_index,
            event_name,
            decoded_params,
            arrayJoin(
                arrayZip(
                    arrayEnumerate(JSONExtract(coalesce(decoded_params['ids'], '[]'), 'Array(String)')),
                    arrayZip(
                        JSONExtract(coalesce(decoded_params['ids'], '[]'), 'Array(String)'),
                        JSONExtract(coalesce(decoded_params['values'], '[]'), 'Array(String)')
                    )
                )
            ) AS batch_tuple
        FROM {{ ref('contracts_circles_v2_StandardTreasury_events') }}
        WHERE event_name = 'GroupRedeemCollateralBurn'
          {% if start_month and end_month %}
            AND toStartOfMonth(toDate(block_timestamp)) >= toDate('{{ start_month }}')
            AND toStartOfMonth(toDate(block_timestamp)) <= toDate('{{ end_month }}')
          {% else %}
            {{ apply_monthly_incremental_filter(source_field='block_timestamp', destination_field='block_timestamp', add_and=true) }}
          {% endif %}
    )
),
batch_returns AS (
    SELECT
        block_number,
        block_timestamp,
        lower(transaction_hash) AS transaction_hash,
        transaction_index,
        log_index,
        batch_tuple.1 AS batch_index,
        event_name,
        lower(decoded_params['group']) AS group_address,
        batch_tuple.2.1 AS token_id,
        -toInt256(toUInt256(batch_tuple.2.2)) AS delta_raw,
        lower(decoded_params['to']) AS recipient
    FROM (
        SELECT
            block_number,
            block_timestamp,
            transaction_hash,
            transaction_index,
            log_index,
            event_name,
            decoded_params,
            arrayJoin(
                arrayZip(
                    arrayEnumerate(JSONExtract(coalesce(decoded_params['ids'], '[]'), 'Array(String)')),
                    arrayZip(
                        JSONExtract(coalesce(decoded_params['ids'], '[]'), 'Array(String)'),
                        JSONExtract(coalesce(decoded_params['values'], '[]'), 'Array(String)')
                    )
                )
            ) AS batch_tuple
        FROM {{ ref('contracts_circles_v2_StandardTreasury_events') }}
        WHERE event_name = 'GroupRedeemCollateralReturn'
          {% if start_month and end_month %}
            AND toStartOfMonth(toDate(block_timestamp)) >= toDate('{{ start_month }}')
            AND toStartOfMonth(toDate(block_timestamp)) <= toDate('{{ end_month }}')
          {% else %}
            {{ apply_monthly_incremental_filter(source_field='block_timestamp', destination_field='block_timestamp', add_and=true) }}
          {% endif %}
    )
)

SELECT * FROM single_locks
UNION ALL
SELECT * FROM batch_locks
UNION ALL
SELECT * FROM batch_burns
UNION ALL
SELECT * FROM batch_returns
