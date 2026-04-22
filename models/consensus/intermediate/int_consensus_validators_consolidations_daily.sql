{% set start_month = var('start_month', none) %}
{% set end_month = var('end_month', none) %}

{{
    config(
        materialized='incremental',
        incremental_strategy=('append' if start_month else 'delete+insert'),
        engine='ReplacingMergeTree()',
        order_by='(date, validator_index, role)',
        unique_key='(date, validator_index, role)',
        partition_by='toStartOfMonth(date)',
        pre_hook=["SET join_algorithm = 'grace_hash'"],
        post_hook=["SET join_algorithm = 'default'"],
        tags=["production", "consensus", "validators_consolidations"]
    )
}}

-- Consolidations (EIP-7251 / MaxEB). See https://notes.ethereum.org/@fradamt/maxeb-consolidation
-- Self-consolidation (source_pubkey == target_pubkey): credential switch 0x01 -> 0x02, no balance transfer.
-- Cross-consolidation (source != target): processed at source's withdrawable epoch; source's full effective
-- balance transfers to target and source exits. We emit the row on the APPLICATION day, inferred as the
-- first day after the request where the source validator's effective balance reaches 0. If application has
-- not yet occurred, no row is emitted (the next run will pick it up).
-- Materialized as a table because (1) consolidations are rare and (2) application day can lag the request by
-- weeks, which breaks monthly-partitioned incremental assumptions.

WITH

requests AS (
    SELECT
        r.slot AS request_slot
        ,toStartOfDay(r.slot_timestamp) AS request_date
        ,lower(JSONExtractString(c, 'source_pubkey')) AS source_pubkey
        ,lower(JSONExtractString(c, 'target_pubkey')) AS target_pubkey
    FROM {{ ref('stg_consensus__execution_requests') }} r
    ARRAY JOIN JSONExtractArrayRaw(payload, 'consolidations') AS c
    WHERE r.slot_timestamp < today()
    {% if start_month and end_month %}
      AND toStartOfMonth(r.slot_timestamp) >= toDate('{{ start_month }}')
      AND toStartOfMonth(r.slot_timestamp) <= toDate('{{ end_month }}')
    {% else %}
      {{ apply_monthly_incremental_filter('r.slot_timestamp', 'date', 'true') }}
    {% endif %}
),

-- Pubkey→validator_index resolution. Uses fct_consensus_validators_status_latest
-- (which reads from stg_consensus__validators_all) instead of int_consensus_validators_labels
-- because labels filters on balance > 0 and therefore excludes exited validators —
-- and the whole point of tracking consolidation SOURCES is that they exit after
-- their balance transfers to the target. Filtering out exited sources dropped every
-- cross-consolidation request on busy application days (observed: 301/302 sources
-- missing on 2025-11-18, producing -533k GNO of spurious "income" loss in aggregate
-- across ~300 orphaned balance-drops).
--
-- Restrict to ONLY the pubkeys that actually appear in this batch's requests. Without
-- this filter the LEFT JOINs below build hash tables of every validator on the network
-- (hundreds of thousands of pubkey strings) — twice — which OOM'd on high-volume
-- batches (e.g. Aug 2025, a peak 0x00→0x02 migration month).
labels AS (
    SELECT validator_index, lower(pubkey) AS pubkey
    FROM {{ ref('fct_consensus_validators_status_latest') }}
    WHERE lower(pubkey) IN (
        SELECT source_pubkey FROM requests
        UNION DISTINCT
        SELECT target_pubkey FROM requests
    )
),

resolved AS (
    SELECT
        req.request_slot
        ,req.request_date
        ,req.source_pubkey
        ,req.target_pubkey
        ,ls.validator_index AS source_validator_index
        ,lt.validator_index AS target_validator_index
    FROM requests req
    LEFT JOIN labels ls ON ls.pubkey = req.source_pubkey
    LEFT JOIN labels lt ON lt.pubkey = req.target_pubkey
),

-- Self-consolidations: credentials-only, emit on request day, no amount.
self_rows AS (
    SELECT
        request_date AS date
        ,source_validator_index AS validator_index
        ,'self' AS role
        ,CAST(NULL AS Nullable(UInt64)) AS counterparty_validator_index
        ,CAST(0 AS Float64) AS transferred_amount_gno
        ,1 AS cnt
    FROM resolved
    WHERE source_pubkey = target_pubkey
      AND source_validator_index IS NOT NULL
),

-- Cross-consolidations: find application day per source validator = first day at/after request where
-- the source's effective_balance_gwei first becomes 0. Transferred amount = the last non-zero effective
-- balance observed strictly before that day (captured via argMax over date when balance>0).
cross_requests AS (
    SELECT *
    FROM resolved
    WHERE source_pubkey != target_pubkey
      AND source_validator_index IS NOT NULL
      AND target_validator_index IS NOT NULL
),

-- Pre-filter snapshots to only the source validators that appear in cross_requests and
-- a TIGHT window around the batch's request dates. Without both bounds, the JOIN would
-- stream months of snapshot history for every source validator through the hash build
-- side and OOM on the shared cluster. Gnosis's churn limit applies cross-consolidations
-- within hours to a few days of the request; 30-day upper bound is generous. The 7-day
-- pre-request lookback keeps the "last non-zero effective balance" lookup correct for
-- same-day applications (see note in `applications` below).
source_snapshots AS (
    SELECT validator_index, date, effective_balance_gwei
    FROM {{ ref('int_consensus_validators_snapshots_daily') }}
    WHERE validator_index IN (SELECT source_validator_index FROM cross_requests)
      AND date >= (SELECT min(request_date) FROM cross_requests) - INTERVAL 7 DAY
      AND date <= (SELECT max(request_date) FROM cross_requests) + INTERVAL 30 DAY
),

applications AS (
    SELECT
        cr.request_slot
        ,cr.source_validator_index
        ,cr.target_validator_index
        ,minIf(s.date, s.effective_balance_gwei = 0 AND s.date >= cr.request_date) AS application_date
        -- Transferred amount = the source validator's last-non-zero effective balance,
        -- regardless of whether that day is before or after the request day. Gnosis's
        -- high churn limit means a consolidation can apply in the SAME slot as the
        -- request (observed: request on day D, eb = 0 also on day D), leaving no
        -- post-request row with eb > 0 to capture. Restricting to `date >= request_date`
        -- missed those same-day applications entirely and produced transferred_amount = 0.
        ,argMaxIf(s.effective_balance_gwei, s.date, s.effective_balance_gwei > 0) AS transferred_amount_gwei
    FROM cross_requests cr
    INNER JOIN source_snapshots s
        ON s.validator_index = cr.source_validator_index
    GROUP BY 1, 2, 3
),

cross_rows AS (
    SELECT
        application_date AS date
        ,source_validator_index AS validator_index
        ,'source' AS role
        ,toNullable(target_validator_index) AS counterparty_validator_index
        ,transferred_amount_gwei / POWER(10, 9) AS transferred_amount_gno
        ,1 AS cnt
    FROM applications
    WHERE application_date IS NOT NULL

    UNION ALL

    SELECT
        application_date AS date
        ,target_validator_index AS validator_index
        ,'target' AS role
        ,toNullable(source_validator_index) AS counterparty_validator_index
        ,transferred_amount_gwei / POWER(10, 9) AS transferred_amount_gno
        ,1 AS cnt
    FROM applications
    WHERE application_date IS NOT NULL
)

SELECT
    date
    ,validator_index
    ,role
    ,counterparty_validator_index
    ,SUM(transferred_amount_gno) AS transferred_amount_gno
    ,SUM(cnt) AS cnt
FROM (
    SELECT * FROM self_rows
    UNION ALL
    SELECT * FROM cross_rows
)
GROUP BY 1, 2, 3, 4
