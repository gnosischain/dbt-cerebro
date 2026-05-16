





WITH journeys AS (
    SELECT
        user_pseudonym,
        identity_role,
        conversion_kind,
        conversion_ts,
        event_kind,
        n_touches,
        first_touch_ts,
        last_touch_ts,
        td_sum
    FROM `dbt`.`fct_execution_gpay_journeys_30d`
    WHERE conversion_date >= today() - INTERVAL 180 DAY
),

per_conv AS (
    SELECT
        user_pseudonym,
        identity_role,
        conversion_kind,
        conversion_ts,
        min(first_touch_ts) AS first_ts_overall,
        max(last_touch_ts)  AS last_ts_overall,
        sum(n_touches)      AS n_touches_total,
        sum(td_sum)         AS td_sum_total,
        arrayCount(t -> t = arrayMin(groupArray(first_touch_ts)), groupArray(first_touch_ts)) AS n_kinds_at_first,
        arrayCount(t -> t = arrayMax(groupArray(last_touch_ts)),  groupArray(last_touch_ts))  AS n_kinds_at_last
    FROM journeys
    GROUP BY
        user_pseudonym,
        identity_role,
        conversion_kind,
        conversion_ts
),

scored AS (
    SELECT
        j.conversion_kind                                                             AS conversion_kind,
        j.identity_role                                                               AS identity_role,
        j.event_kind                                                                  AS event_kind,
        j.user_pseudonym                                                              AS user_pseudonym,
        j.conversion_ts                                                               AS conversion_ts,
        if(j.first_touch_ts = pc.first_ts_overall,
           1.0 / nullIf(pc.n_kinds_at_first, 0),
           0.0)                                                                       AS first_touch_credit,
        if(j.last_touch_ts  = pc.last_ts_overall,
           1.0 / nullIf(pc.n_kinds_at_last,  0),
           0.0)                                                                       AS last_touch_credit,
        toFloat64(j.n_touches) / nullIf(pc.n_touches_total, 0)                        AS linear_credit,
        j.td_sum               / nullIf(pc.td_sum_total, 0)                           AS time_decay_credit
    FROM journeys j
    INNER JOIN per_conv pc
        ON  pc.user_pseudonym  = j.user_pseudonym
        AND pc.identity_role   = j.identity_role
        AND pc.conversion_kind = j.conversion_kind
        AND pc.conversion_ts   = j.conversion_ts
),

totals AS (
    SELECT
        conversion_kind,
        identity_role,
        count() AS total_conversions
    FROM `dbt`.`int_execution_gpay_conversions`
    WHERE conversion_date >= today() - INTERVAL 180 DAY
    GROUP BY
        conversion_kind,
        identity_role
)

SELECT
    s.conversion_kind                                AS conversion_kind,
    s.identity_role                                  AS identity_role,
    s.event_kind                                     AS event_kind,
    uniqExact(s.user_pseudonym, s.conversion_ts)     AS conversions_with_touch,
    sum(first_touch_credit)                          AS first_touch,
    sum(last_touch_credit)                           AS last_touch,
    sum(linear_credit)                               AS linear,
    sum(time_decay_credit)                           AS time_decay_hl_7d,
    any(t.total_conversions)                         AS total_conversions,
    now()                                            AS computed_at
FROM scored s
LEFT JOIN totals t
    ON  t.conversion_kind = s.conversion_kind
    AND t.identity_role   = s.identity_role
GROUP BY
    s.conversion_kind,
    s.identity_role,
    s.event_kind
