{% set start_month = var('start_month', none) %}
{% set end_month   = var('end_month', none) %}

{{
    config(
        materialized='incremental',
        incremental_strategy=('append' if (start_month or var('incremental_end_date', none)) else 'delete+insert'),
        engine='ReplacingMergeTree()',
        order_by='avatar',
        unique_key='avatar',
        settings={'allow_nullable_key': 1},
        tags=['production', 'execution', 'circles_v2', 'avatar_metadata'],
        pre_hook=[
            "SET allow_experimental_json_type = 1",
            "SET max_memory_usage = 5000000000",
            "SET max_bytes_before_external_sort = 1500000000",
            "SET max_bytes_before_external_group_by = 1500000000",
            "SET max_threads = 1",
            "SET max_block_size = 16",
            "SET preferred_block_size_bytes = 4194304"
        ],
        post_hook=[
            "SET allow_experimental_json_type = 0",
            "SET max_memory_usage = 0",
            "SET max_bytes_before_external_sort = 0",
            "SET max_bytes_before_external_group_by = 0",
            "SET max_block_size = 65505",
            "SET preferred_block_size_bytes = 1000000"
        ]
    )
}}
-- Current parsed IPFS metadata for every Circles v2 avatar.
-- Joins the latest known (avatar, metadata_digest) pair from
-- `int_execution_circles_v2_avatar_metadata_targets` to the most
-- recently fetched payload in `circles_avatar_metadata_raw` and
-- exposes a small set of typed fields for downstream models.
--
-- Incremental: keyed on `avatar`. On each run we re-derive only the
-- avatars whose metadata has changed since the last run — either a new
-- avatar appeared, the current target's digest changed, or a fresher
-- (avatar, digest) payload was fetched. delete+insert with
-- unique_key='avatar' guarantees one final row per avatar regardless of
-- how many incremental runs touched it. Full-refresh is required after
-- deploy to migrate from the previous `table` materialization.

{% if is_incremental() %}
{% set last_fetched_at %}
  (SELECT coalesce(max(metadata_fetched_at), toDateTime('1970-01-01'))
   FROM {{ this }})
{% endset %}
{% endif %}

-- Avatars in scope this run.
--
-- Three filtering modes drive `scoped_avatars`:
--   1. `start_month`+`end_month` set (full-refresh batched runner) →
--      avatars whose registration block_timestamp falls in that month.
--      Each month-batch processes only that month's new avatars in append
--      mode; ReplacingMergeTree dedups on `unique_key=avatar`.
--   2. `is_incremental()` (daily delete+insert) → union of three sets,
--      none of which require reading the body column:
--        a. NEW avatars (in upstream avatars table but not in this table)
--        b. DIGEST-CHANGED avatars (current_target.metadata_digest differs
--           from this.metadata_digest)
--        c. FRESH-FETCH avatars (any raw fetch with fetched_at >
--           max(this.metadata_fetched_at) — cheap, only reads avatar +
--           fetched_at + http_status columns)
--      Without this scope, an incremental run scans all 19k avatars'
--      raw fetch attempts and OOMs on the body column read.
--   3. Neither set (first build / --full-refresh in one shot) → all avatars.
--
-- `scoped_avatars` is defined FIRST so we can push the avatar filter
-- into the raw-source scan and avoid loading body for out-of-scope
-- avatars (the source has hundreds of fetch attempts per avatar; the
-- body column is MB-scale and OOMs the cluster otherwise).
WITH
{% if is_incremental() and not (start_month and end_month) %}
incremental_watermark AS (
    SELECT
        coalesce(max(metadata_fetched_at), toDateTime('1970-01-01'))
            AS last_metadata_fetched_at
    FROM {{ this }}
),
{% endif %}
scoped_avatars AS (
{% if start_month and end_month %}
    -- Full-refresh batched mode: avatars registered in the month range.
    SELECT a.avatar AS avatar
    FROM {{ ref('int_execution_circles_v2_avatars') }} a
    WHERE toStartOfMonth(toDate(a.block_timestamp)) >= toDate('{{ start_month }}')
      AND toStartOfMonth(toDate(a.block_timestamp)) <= toDate('{{ end_month }}')
    GROUP BY a.avatar
{% elif is_incremental() %}
    -- Incremental mode: only avatars that changed since last run. None of
    -- the three branches read the body column.
    SELECT DISTINCT avatar FROM (
        -- (a) New avatars: in upstream but not in this table yet.
        SELECT a.avatar AS avatar
        FROM {{ ref('int_execution_circles_v2_avatars') }} a
        LEFT JOIN {{ this }} prev ON prev.avatar = a.avatar
        WHERE prev.avatar IS NULL

        UNION DISTINCT

        -- (b) Digest changed: current_target points to a digest different
        -- from what we wrote previously.
        SELECT t.avatar AS avatar
        FROM {{ ref('int_execution_circles_v2_avatar_metadata_targets') }} t
        LEFT JOIN {{ this }} prev ON prev.avatar = t.avatar
        WHERE t.is_current_avatar_metadata
          AND coalesce(prev.metadata_digest, '') != coalesce(t.metadata_digest, '')

        UNION DISTINCT

        -- (c) Fresh fetch: a 200/non-empty fetch arrived since the last
        -- write watermark. Reads only `avatar` + `fetched_at` + filter
        -- columns; body column is NOT read here.
        SELECT r.avatar AS avatar
        FROM {{ source('auxiliary', 'circles_avatar_metadata_raw') }} r
        WHERE r.http_status = 200
          AND r.body != ''
          AND r.fetched_at > (SELECT last_metadata_fetched_at FROM incremental_watermark)
    )
{% else %}
    -- First build (no incremental table yet, no start_month): all avatars.
    SELECT a.avatar AS avatar
    FROM {{ ref('int_execution_circles_v2_avatars') }} a
    GROUP BY a.avatar
{% endif %}
),
current_target AS (
    SELECT *
    FROM {{ ref('int_execution_circles_v2_avatar_metadata_targets') }}
    WHERE is_current_avatar_metadata
      AND avatar IN (SELECT avatar FROM scoped_avatars)
),
-- Two-pass aggregation to avoid loading the (large) `body` column for
-- every fetch attempt:
--
--   Pass 1 (latest_fetch_times) — scans only `avatar`, `metadata_digest`,
--   `fetched_at` to find the latest fetch per (avatar, digest). These
--   are small columns; can stream through cheaply.
--
--   Pass 2 (latest_raw) — INNER JOIN back to the raw source on the exact
--   (avatar, digest, fetched_at) tuple, so we only read the MB-scale
--   `body` / `content_type` columns for the surviving rows.
--
-- Both passes filter to `avatar IN scoped_avatars` so the source scan
-- doesn't even read fetches for avatars outside this batch's window.
-- A single-pass argMax over the body OOMed on MergeTreeSelect when CH
-- read the body column across all fetches.
latest_fetch_times AS (
    SELECT
        avatar,
        metadata_digest,
        max(fetched_at) AS max_fetched_at
    FROM (
        SELECT avatar, metadata_digest, fetched_at
        FROM {{ source('auxiliary', 'circles_avatar_metadata_raw') }}
        WHERE http_status = 200
          AND body != ''
          AND avatar IN (SELECT avatar FROM scoped_avatars)
    )
    GROUP BY avatar, metadata_digest
),
latest_raw AS (
    SELECT
        r.avatar         AS avatar,
        r.metadata_digest AS metadata_digest,
        r.http_status    AS http_status_latest,
        r.content_type   AS content_type_latest,
        r.body           AS body_latest,
        r.fetched_at     AS fetched_at_latest
    FROM {{ source('auxiliary', 'circles_avatar_metadata_raw') }} r
    INNER JOIN latest_fetch_times lft
        ON r.avatar = lft.avatar
       AND r.metadata_digest = lft.metadata_digest
       AND r.fetched_at = lft.max_fetched_at
    WHERE r.http_status = 200
      AND r.body != ''
      AND r.avatar IN (SELECT avatar FROM scoped_avatars)
),
-- Parse the JSON body once per row into a typed Tuple, then project
-- fields by index. A naive `JSONExtractString(body, 'name')`,
-- `JSONExtractString(body, 'symbol')`, ... pattern re-parses the body
-- for each call, multiplying memory by the number of extracted fields
-- — OOMs at 7.2 GiB for MB-scale IPFS payloads.
parsed AS (
    SELECT
        r.avatar                                          AS avatar,
        r.metadata_digest                                 AS metadata_digest,
        r.body_latest                                     AS metadata_body,
        r.fetched_at_latest                               AS metadata_fetched_at,
        JSONExtract(
            r.body_latest,
            'Tuple(name String, symbol String, description String, imageUrl String, previewImageUrl String)'
        ) AS parsed_body
    FROM latest_raw r
)
SELECT
    a.avatar                                            AS avatar,
    a.avatar_type                                       AS avatar_type,
    a.name                                              AS onchain_name,
    t.metadata_digest                                   AS metadata_digest,
    t.ipfs_cid_v0                                       AS ipfs_cid_v0,
    t.gateway_url                                       AS gateway_url,
    p.parsed_body.1                                     AS metadata_name,
    p.parsed_body.2                                     AS metadata_symbol,
    p.parsed_body.3                                     AS metadata_description,
    p.parsed_body.4                                     AS metadata_image_url,
    p.parsed_body.5                                     AS metadata_preview_image_url,
    p.metadata_body                                     AS metadata_body,
    p.metadata_fetched_at                               AS metadata_fetched_at
FROM {{ ref('int_execution_circles_v2_avatars') }} a
INNER JOIN scoped_avatars s ON s.avatar = a.avatar
LEFT JOIN current_target t
    ON a.avatar = t.avatar
LEFT JOIN parsed p
    ON t.avatar = p.avatar
   AND t.metadata_digest = p.metadata_digest
