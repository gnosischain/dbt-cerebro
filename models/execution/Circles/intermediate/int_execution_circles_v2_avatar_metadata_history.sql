{{
    config(
        materialized='incremental',
        incremental_strategy=('append' if var('start_month', none) else 'delete+insert'),
        engine='ReplacingMergeTree()',
        order_by='(avatar, valid_from, log_index)',
        unique_key='(avatar, metadata_digest)',
        partition_by='toStartOfMonth(valid_from)',
        settings={'allow_nullable_key': 1},
        pre_hook=["SET allow_experimental_json_type = 1"],
        tags=['production', 'execution', 'circles_v2', 'avatar_metadata']
    )
}}

-- SCD-style historical view of every Circles v2 avatar metadata change.
--
-- One row per (avatar, metadata_digest) ever announced by the
-- NameRegistry, in event order, with valid_from / valid_to / is_current
-- columns so the dashboard can render the full timeline of name + image
-- changes for a selected avatar.
--
-- Incremental strategy: delete+insert keyed on (avatar, metadata_digest).
-- The `affected_avatars` CTE uses the standard
-- `apply_monthly_incremental_filter` macro (same as other Circles
-- intermediates) to find any avatar that received an UpdateMetadataDigest
-- event in the incremental window. We then INNER JOIN the targets view
-- to that set so the full history of every affected avatar is
-- recomputed in one shot — guaranteeing the leadInFrame window function
-- still sees every event for those avatars and `valid_to` stays correct.
--
-- valid_to is NULL on the most recent row per avatar.

{% set start_month = var('start_month', none) %}
{% set end_month = var('end_month', none) %}

WITH affected_avatars AS (
    SELECT DISTINCT avatar
    FROM {{ ref('int_execution_circles_v2_avatar_metadata_targets') }}
    WHERE 1 = 1
      {% if start_month and end_month %}
        AND toStartOfMonth(toDate(block_timestamp)) >= toDate('{{ start_month }}')
        AND toStartOfMonth(toDate(block_timestamp)) <= toDate('{{ end_month }}')
      {% else %}
        {{ apply_monthly_incremental_filter(
            source_field='block_timestamp',
            destination_field='valid_from',
            add_and=true
        ) }}
      {% endif %}
),
ordered AS (
    SELECT
        t.avatar,
        t.metadata_digest,
        t.ipfs_cid_v0,
        t.gateway_url,
        t.block_timestamp,
        t.transaction_hash,
        t.log_index,
        t.is_current_avatar_metadata,
        leadInFrame(t.block_timestamp) OVER (
            PARTITION BY t.avatar
            ORDER BY t.block_timestamp, t.log_index
            ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
        ) AS next_block_timestamp
    FROM {{ ref('int_execution_circles_v2_avatar_metadata_targets') }} t
    INNER JOIN affected_avatars aa
        ON aa.avatar = t.avatar
),
latest_raw AS (
    SELECT
        avatar,
        metadata_digest,
        http_status,
        content_type,
        body,
        fetched_at,
        row_number() OVER (
            PARTITION BY avatar, metadata_digest
            ORDER BY fetched_at DESC
        ) AS rn
    FROM {{ source('auxiliary', 'circles_avatar_metadata_raw') }}
    WHERE http_status = 200
      AND body != ''
)
SELECT
    a.avatar                                            AS avatar,
    a.avatar_type                                       AS avatar_type,
    a.name                                              AS onchain_name,
    o.metadata_digest                                   AS metadata_digest,
    o.ipfs_cid_v0                                       AS ipfs_cid_v0,
    o.gateway_url                                       AS gateway_url,
    o.block_timestamp                                   AS valid_from,
    if(o.next_block_timestamp = toDateTime64('1970-01-01 00:00:00', 0, 'UTC'),
       NULL, o.next_block_timestamp)                    AS valid_to,
    o.is_current_avatar_metadata                        AS is_current,
    o.transaction_hash                                  AS transaction_hash,
    o.log_index                                         AS log_index,
    JSONExtractString(r.body, 'name')                   AS metadata_name,
    JSONExtractString(r.body, 'symbol')                 AS metadata_symbol,
    JSONExtractString(r.body, 'description')            AS metadata_description,
    JSONExtractString(r.body, 'imageUrl')               AS metadata_image_url,
    JSONExtractString(r.body, 'previewImageUrl')        AS metadata_preview_image_url,
    r.body                                              AS metadata_body,
    r.fetched_at                                        AS metadata_fetched_at
FROM ordered o
INNER JOIN {{ ref('int_execution_circles_v2_avatars') }} a
    ON a.avatar = o.avatar
LEFT JOIN latest_raw r
    ON o.avatar = r.avatar
   AND o.metadata_digest = r.metadata_digest
   AND r.rn = 1
