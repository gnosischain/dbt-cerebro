

WITH unioned AS (
    SELECT truster AS avatar, trustee AS counterparty, 'outgoing' AS direction, valid_from
    FROM `dbt`.`fct_execution_circles_v2_trust_relations_current`
    WHERE truster IS NOT NULL

    UNION ALL

    SELECT trustee AS avatar, truster AS counterparty, 'incoming' AS direction, valid_from
    FROM `dbt`.`fct_execution_circles_v2_trust_relations_current`
    WHERE trustee IS NOT NULL
),
agg AS (
    SELECT
        avatar,
        counterparty,
        countIf(direction = 'outgoing') AS out_cnt,
        countIf(direction = 'incoming') AS in_cnt,
        maxIf(valid_from, direction = 'outgoing') AS outgoing_from_raw,
        maxIf(valid_from, direction = 'incoming') AS incoming_from_raw
    FROM unioned
    GROUP BY avatar, counterparty
)

SELECT
    avatar,
    counterparty,
    multiIf(
        out_cnt > 0 AND in_cnt > 0, 'mutual',
        out_cnt > 0, 'outgoing',
        'incoming'
    ) AS direction,
    if(out_cnt > 0, outgoing_from_raw, NULL) AS outgoing_from,
    if(in_cnt > 0, incoming_from_raw, NULL) AS incoming_from
FROM agg