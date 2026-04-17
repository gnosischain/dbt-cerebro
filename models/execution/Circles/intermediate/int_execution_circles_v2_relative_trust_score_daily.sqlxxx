{{
    config(
        materialized='incremental',
        incremental_strategy=('append' if var('start_month', none) else 'delete+insert'),
        engine='ReplacingMergeTree()',
        order_by='(date, avatar)',
        unique_key='(date, avatar)',
        partition_by='toStartOfMonth(date)',
        settings={'allow_nullable_key': 1},
        tags=['dev', 'execution', 'circles_v2', 'trust_score']
    )
}}

-- depends_on: {{ ref('int_execution_circles_v2_avatars') }}
-- depends_on: {{ ref('int_execution_circles_v2_trust_pair_ranges') }}

{# ── Parameters ──────────────────────────────────────────── #}
{% set start_month      = var('start_month', none) %}
{% set end_month        = var('end_month',   none) %}
{% set target_group     = var('circles_target_group_address',
                              '0x1aca75e38263c79d9d4f10df0635cc6fcfe6f026') %}
{% set group_start_date = var('circles_target_group_start_date', '2025-04-25') %}
{% set penetration_w    = var('circles_penetration_weight', 0.2) %}
{% set quality_w        = var('circles_quality_weight', 0.8) %}
{% set decay            = var('circles_hop_decay', [1.0, 0.9, 0.8, 0.6, 0.4, 0.2]) %}
{% set max_hops         = decay | length - 1 %}

{# ── Resolve date list via run_query ─────────────────────── #}
{#
  BFS memory scales with (backers × reachable_nodes) per date.
  Processing multiple dates in a single query multiplies the
  visited-set hash tables and OOMs on ClickHouse Cloud (10 GiB).
  Instead, we resolve dates at Jinja compile time and generate
  one self-contained BFS subquery per date, joined with UNION ALL.
  ClickHouse pipelines UNION ALL branches sequentially, so only
  one dates BFS is resident in memory at a time.
#}
{% if execute %}
    {% if start_month and end_month %}
        {% set dates_query %}
            SELECT toDate(addDays(
                toDate('{{ start_month }}'), offset
            )) AS d
            FROM system.numbers
            ARRAY JOIN range(
                toUInt32(greatest(
                    dateDiff('day',
                        toDate('{{ start_month }}'),
                        least(toLastDayOfMonth(toDate('{{ end_month }}')), yesterday())
                    ) + 1, 0
                ))
            ) AS offset
            ORDER BY d
        {% endset %}
    {% elif is_incremental() %}
        {% set dates_query %}
            SELECT toDate(addDays(max_d, 1 + offset)) AS d
            FROM (
                SELECT max(date) AS max_d
                FROM {{ this }}
                WHERE date < yesterday()
            )
            ARRAY JOIN range(
                toUInt32(greatest(
                    dateDiff('day', max_d, yesterday()), 0
                ))
            ) AS offset
            ORDER BY d
        {% endset %}
    {% else %}
        {% set dates_query %}
            SELECT toDate(addDays(
                toDate('{{ group_start_date }}'), offset
            )) AS d
            FROM system.numbers
            ARRAY JOIN range(
                toUInt32(greatest(
                    dateDiff('day',
                        toDate('{{ group_start_date }}'),
                        yesterday()
                    ) + 1, 0
                ))
            ) AS offset
            ORDER BY d
        {% endset %}
    {% endif %}

    {% set dates_result = run_query(dates_query) %}
    {% set dates = dates_result.columns[0].values() %}
{% else %}
    {% set dates = [] %}
{% endif %}

{# ── Guard: nothing to process ───────────────────────────── #}
{% if dates | length == 0 %}

SELECT
    toDate('{{ group_start_date }}') AS date,
    ''                               AS avatar,
    toFloat32(0)                     AS relative_trust_score,
    toUInt16(0)                      AS targets_reached,
    toUInt16(0)                      AS total_targets,
    toFloat32(0)                     AS penetration_rate
WHERE 0

{% else %}

{# ── Generate one BFS subquery per date ──────────────────── #}
{% for score_date in dates %}
{% set d = score_date | string %}
{% if not loop.first %}UNION ALL{% endif %}

SELECT
    toDate('{{ d }}') AS date,
    avatar,
    relative_trust_score,
    targets_reached,
    total_targets,
    penetration_rate
FROM (

    WITH

    humans AS (
        SELECT DISTINCT avatar
        FROM {{ ref('int_execution_circles_v2_avatars') }}
        WHERE avatar_type = 'Human'
    ),

    -- Active human-to-human trust edges on this specific date
    edges AS (
        SELECT tp.truster, tp.trustee
        FROM {{ ref('int_execution_circles_v2_trust_pair_ranges') }} tp
        WHERE tp.truster IN (SELECT avatar FROM humans)
          AND tp.trustee IN (SELECT avatar FROM humans)
          AND arrayExists(
              (vf, vt) -> vf < toDateTime('{{ d }}') + INTERVAL 1 DAY
                       AND vt > toDateTime('{{ d }}'),
              tp.valid_from_agg,
              tp.valid_to_agg
          )
    ),

    -- Backers = humans trusted by the target group on this date
    backers AS (
        SELECT trustee AS backer
        FROM edges
        WHERE truster = '{{ target_group }}'
          AND trustee != '{{ target_group }}'
    ),

    total_backers AS (
        SELECT toUInt16(count()) AS cnt FROM backers
    ),

    -- ── BFS {{ max_hops + 1 }} levels (hop 0-{{ max_hops }}) ──

    l0 AS (
        SELECT backer, backer AS node
        FROM backers
    ),

    {% for hop in range(1, max_hops + 1) %}
    l{{ hop }} AS (
        SELECT DISTINCT l.backer, e.trustee AS node
        FROM l{{ hop - 1 }} l
        INNER JOIN edges e ON l.node = e.truster
        WHERE (l.backer, e.trustee) NOT IN (
            SELECT backer, node
            FROM {% if hop == 1 %}l0{% else %}v{{ hop - 1 }}{% endif %}
        )
    ),

    {% if hop < max_hops %}
    v{{ hop }} AS (
        SELECT backer, node
        FROM {% if hop == 1 %}l0{% else %}v{{ hop - 1 }}{% endif %}
        UNION ALL
        SELECT backer, node FROM l{{ hop }}
    ),
    {% endif %}

    {% endfor %}

    -- ── Aggregate per node ──

    node_agg AS (
        SELECT
            node,
            sum(cnt)             AS targets_reached,
            sum(cnt * decay_val) AS weighted_sum
        FROM (
            {% for hop in range(max_hops + 1) %}
            {% if not loop.first %}UNION ALL{% endif %}
            SELECT node, count() AS cnt, {{ decay[hop] }} AS decay_val
            FROM l{{ hop }} GROUP BY node
            {% endfor %}
        )
        GROUP BY node
    )

    -- ── Score ──

    SELECT
        na.node                                              AS avatar,
        toFloat32(round(least(100.0, greatest(0.0,
            (na.targets_reached / tb.cnt) * 100.0 * {{ penetration_w }}
          + (na.weighted_sum / na.targets_reached) * 100.0 * {{ quality_w }}
        )), 2))                                              AS relative_trust_score,
        toUInt16(na.targets_reached)                         AS targets_reached,
        tb.cnt                                               AS total_targets,
        toFloat32(round(na.targets_reached / tb.cnt, 4))     AS penetration_rate
    FROM node_agg na
    CROSS JOIN total_backers tb
    WHERE na.node IN (SELECT avatar FROM humans)
      AND na.node != '{{ target_group }}'
      AND tb.cnt > 0

)

{% endfor %}

{% endif %}
