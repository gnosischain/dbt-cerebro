

WITH exploded AS (
    SELECT
        truster,
        trustee,
        r.1 AS valid_from,
        r.2 AS valid_to
    FROM `dbt`.`int_execution_circles_v2_trust_pair_ranges`
    ARRAY JOIN arrayZip(valid_from_agg, valid_to_agg) AS r
),

daily_changes AS (
    -- outgoing +1 on start
    SELECT
        truster AS avatar,
        toDate(valid_from) AS day,
        toInt64(1) AS out_delta,
        toInt64(0) AS in_delta
    FROM exploded

    UNION ALL

    -- outgoing -1 on end
    SELECT
        truster AS avatar,
        toDate(valid_to) AS day,
        toInt64(-1) AS out_delta,
        toInt64(0) AS in_delta
    FROM exploded
    WHERE valid_to < toDateTime('2106-02-07 06:28:15')

    UNION ALL

    -- incoming +1 on start
    SELECT
        trustee AS avatar,
        toDate(valid_from) AS day,
        toInt64(0) AS out_delta,
        toInt64(1) AS in_delta
    FROM exploded

    UNION ALL

    -- incoming -1 on end
    SELECT
        trustee AS avatar,
        toDate(valid_to) AS day,
        toInt64(0) AS out_delta,
        toInt64(-1) AS in_delta
    FROM exploded
    WHERE valid_to < toDateTime('2106-02-07 06:28:15')
),

collapsed AS (
    SELECT
        avatar,
        day,
        sum(out_delta) AS out_delta,
        sum(in_delta) AS in_delta
    FROM daily_changes
    GROUP BY
        avatar,
        day
),

bounds AS (
    SELECT
        avatar,
        min(day) AS min_day,
        today() AS max_day
    FROM collapsed
    GROUP BY avatar
),

calendar AS (
    SELECT
        avatar,
        addDays(min_day, n) AS day
    FROM bounds
    ARRAY JOIN range(dateDiff('day', min_day, max_day) + 1) AS n
),

dense AS (
    SELECT
        c.avatar,
        c.day,
        coalesce(ch.out_delta, toInt64(0)) AS out_delta,
        coalesce(ch.in_delta, toInt64(0)) AS in_delta
    FROM calendar c
    LEFT JOIN collapsed ch
        ON c.avatar = ch.avatar
       AND c.day = ch.day
)

SELECT
    avatar,
    day,
    sum(out_delta) OVER (
        PARTITION BY avatar
        ORDER BY day
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS trusts_given_count,
    sum(in_delta) OVER (
        PARTITION BY avatar
        ORDER BY day
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS trusts_received_count
FROM dense
ORDER BY avatar, day