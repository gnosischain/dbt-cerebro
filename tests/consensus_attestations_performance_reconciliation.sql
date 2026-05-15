-- Reconciles fct_consensus_attestations_performance_daily.attestations_total
-- against SUM(cnt) from the underlying histogram int_consensus_attestations_daily,
-- and asserts the inclusion-delay shares are in [0, 1] and sum to ~1.

WITH

histogram AS (
    SELECT date, SUM(cnt) AS total_from_histogram
    FROM {{ ref('int_consensus_attestations_daily') }}
    GROUP BY date
),

kpis AS (
    SELECT *
    FROM {{ ref('fct_consensus_attestations_performance_daily') }}
),

joined AS (
    SELECT
        k.date
        ,k.attestations_total
        ,h.total_from_histogram
        ,k.pct_inclusion_distance_1
        ,k.pct_inclusion_distance_le_2
        ,k.pct_inclusion_distance_gt_1
    FROM kpis k
    INNER JOIN histogram h ON h.date = k.date
)

SELECT *
FROM joined
WHERE
    {% if var('test_full_refresh', false) %}1=1
    {% else %}toDate(date) >= today() - {{ var('test_lookback_days', 7) }}
    {% endif %}
    AND (
        attestations_total != total_from_histogram
        OR pct_inclusion_distance_1 < 0 OR pct_inclusion_distance_1 > 1
        OR pct_inclusion_distance_le_2 < 0 OR pct_inclusion_distance_le_2 > 1
        OR pct_inclusion_distance_gt_1 < 0 OR pct_inclusion_distance_gt_1 > 1
        OR ABS(pct_inclusion_distance_1 + pct_inclusion_distance_gt_1 - 1.0) > 1e-6
    )
