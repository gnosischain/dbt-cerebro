{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        engine='ReplacingMergeTree()',
        order_by='(block_timestamp, transaction_hash, log_index)',
        partition_by='toStartOfMonth(block_timestamp)',
        settings={'allow_nullable_key': 1},
        tags=['production', 'execution', 'circles_v2', 'avatars'],
        pre_hook=["SET join_use_nulls = 1"],
        post_hook=["SET join_use_nulls = 0"]
    )
}}

{% set start_month = var('start_month', none) %}
{% set end_month = var('end_month', none) %}
SELECT
    block_number,
    block_timestamp,
    lower(concat('0x', transaction_hash)) AS transaction_hash,
    transaction_index,
    log_index,
    CASE
        WHEN event_name = 'RegisterHuman' THEN 'Human'
        WHEN event_name = 'RegisterGroup' THEN 'Group'
        WHEN event_name = 'RegisterOrganization' THEN 'Org'
        ELSE 'Unknown'
    END AS avatar_type,
    -- canonical inviter (invitation-at-scale farm proxy remapped to origin);
    -- see int_execution_circles_v2_inviter_canonical
    CASE
        WHEN event_name = 'RegisterHuman' THEN ic.canonical_inviter
        ELSE NULL
    END AS invited_by,
    lower(
        CASE
            WHEN event_name = 'RegisterHuman' THEN decoded_params['avatar']
            WHEN event_name = 'RegisterGroup' THEN decoded_params['group']
            ELSE decoded_params['organization']
        END
    ) AS avatar,
    lower(
        CASE
            WHEN event_name = 'RegisterHuman' THEN decoded_params['avatar']
            WHEN event_name = 'RegisterGroup' THEN decoded_params['group']
            ELSE NULL
        END
    ) AS token_id,
    CASE
        WHEN event_name IN ('RegisterGroup', 'RegisterOrganization') THEN decoded_params['name']
        ELSE NULL
    END AS name
FROM {{ ref('contracts_circles_v2_Hub_events') }} h
LEFT JOIN {{ ref('int_execution_circles_v2_inviter_canonical') }} ic
    ON ic.avatar = lower(h.decoded_params['avatar'])
WHERE
    event_name IN ('RegisterHuman','RegisterGroup','RegisterOrganization')
    {% if start_month and end_month %}
      AND toStartOfMonth(toDate(block_timestamp)) >= toDate('{{ start_month }}')
      AND toStartOfMonth(toDate(block_timestamp)) <= toDate('{{ end_month }}')
    {% else %}
      {{ apply_monthly_incremental_filter(source_field='block_timestamp', destination_field='block_timestamp', add_and=true) }}
    {% endif %}
