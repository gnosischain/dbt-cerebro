








  








WITH cutoff AS (
    SELECT coalesce(max(week), toDate('1970-01-01')) - INTERVAL 4 WEEK AS cutoff_week
    FROM `dbt`.`int_revenue_fees_weekly_per_user`
)





SELECT
    week,
    'holdings' AS stream_type,
    symbol,
    user,
    week_fees,
    annual_rolling_fees
FROM (
    WITH weekly_sparse AS (
        SELECT
            toStartOfWeek(date, 1) AS week,
            user,
            symbol,
            round(sum(fees), 8) AS week_fees
        FROM `dbt`.`int_revenue_holdings_fees_daily`
        WHERE date < toStartOfWeek(today(), 1)
          AND symbol = 'EURe'
          
            AND toStartOfWeek(date, 1) >= (SELECT cutoff_week FROM cutoff) - INTERVAL 52 WEEK
          
        GROUP BY week, user, symbol
    ),
    user_span AS (
        SELECT
            user,
            symbol,
            
              greatest(min(week),
                       (SELECT cutoff_week FROM cutoff) - INTERVAL 52 WEEK) AS first_week,
              toStartOfWeek(today(), 1) - INTERVAL 1 WEEK AS last_week
            
        FROM weekly_sparse
        GROUP BY 1, 2
    ),
    calendar AS (
        SELECT
            user,
            symbol,
            arrayJoin(
                arrayMap(
                    i -> toDate(addWeeks(first_week, i)),
                    range(toUInt32(dateDiff('week', first_week, last_week) + 1))
                )
            ) AS week
        FROM user_span
    ),
    weekly_dense AS (
        SELECT
            c.week,
            c.user,
            c.symbol,
            coalesce(ws.week_fees, 0) AS week_fees
        FROM calendar c
        LEFT JOIN weekly_sparse ws
            ON  ws.week   = c.week
            AND ws.user   = c.user
            AND ws.symbol = c.symbol
    )
    SELECT
        week,
        user,
        symbol,
        week_fees,
        round(
            sum(week_fees) OVER (
                PARTITION BY user
                ORDER BY week
                ROWS BETWEEN 51 PRECEDING AND CURRENT ROW
            ),
            8
        ) AS annual_rolling_fees
    FROM weekly_dense
) s
WHERE (s.annual_rolling_fees > 0 OR s.week_fees > 0)
  
    AND s.week > (SELECT cutoff_week FROM cutoff)
  


UNION ALL


SELECT
    week,
    'holdings' AS stream_type,
    symbol,
    user,
    week_fees,
    annual_rolling_fees
FROM (
    WITH weekly_sparse AS (
        SELECT
            toStartOfWeek(date, 1) AS week,
            user,
            symbol,
            round(sum(fees), 8) AS week_fees
        FROM `dbt`.`int_revenue_holdings_fees_daily`
        WHERE date < toStartOfWeek(today(), 1)
          AND symbol = 'USDC.e'
          
            AND toStartOfWeek(date, 1) >= (SELECT cutoff_week FROM cutoff) - INTERVAL 52 WEEK
          
        GROUP BY week, user, symbol
    ),
    user_span AS (
        SELECT
            user,
            symbol,
            
              greatest(min(week),
                       (SELECT cutoff_week FROM cutoff) - INTERVAL 52 WEEK) AS first_week,
              toStartOfWeek(today(), 1) - INTERVAL 1 WEEK AS last_week
            
        FROM weekly_sparse
        GROUP BY 1, 2
    ),
    calendar AS (
        SELECT
            user,
            symbol,
            arrayJoin(
                arrayMap(
                    i -> toDate(addWeeks(first_week, i)),
                    range(toUInt32(dateDiff('week', first_week, last_week) + 1))
                )
            ) AS week
        FROM user_span
    ),
    weekly_dense AS (
        SELECT
            c.week,
            c.user,
            c.symbol,
            coalesce(ws.week_fees, 0) AS week_fees
        FROM calendar c
        LEFT JOIN weekly_sparse ws
            ON  ws.week   = c.week
            AND ws.user   = c.user
            AND ws.symbol = c.symbol
    )
    SELECT
        week,
        user,
        symbol,
        week_fees,
        round(
            sum(week_fees) OVER (
                PARTITION BY user
                ORDER BY week
                ROWS BETWEEN 51 PRECEDING AND CURRENT ROW
            ),
            8
        ) AS annual_rolling_fees
    FROM weekly_dense
) s
WHERE (s.annual_rolling_fees > 0 OR s.week_fees > 0)
  
    AND s.week > (SELECT cutoff_week FROM cutoff)
  


UNION ALL


SELECT
    week,
    'holdings' AS stream_type,
    symbol,
    user,
    week_fees,
    annual_rolling_fees
FROM (
    WITH weekly_sparse AS (
        SELECT
            toStartOfWeek(date, 1) AS week,
            user,
            symbol,
            round(sum(fees), 8) AS week_fees
        FROM `dbt`.`int_revenue_holdings_fees_daily`
        WHERE date < toStartOfWeek(today(), 1)
          AND symbol = 'BRLA'
          
            AND toStartOfWeek(date, 1) >= (SELECT cutoff_week FROM cutoff) - INTERVAL 52 WEEK
          
        GROUP BY week, user, symbol
    ),
    user_span AS (
        SELECT
            user,
            symbol,
            
              greatest(min(week),
                       (SELECT cutoff_week FROM cutoff) - INTERVAL 52 WEEK) AS first_week,
              toStartOfWeek(today(), 1) - INTERVAL 1 WEEK AS last_week
            
        FROM weekly_sparse
        GROUP BY 1, 2
    ),
    calendar AS (
        SELECT
            user,
            symbol,
            arrayJoin(
                arrayMap(
                    i -> toDate(addWeeks(first_week, i)),
                    range(toUInt32(dateDiff('week', first_week, last_week) + 1))
                )
            ) AS week
        FROM user_span
    ),
    weekly_dense AS (
        SELECT
            c.week,
            c.user,
            c.symbol,
            coalesce(ws.week_fees, 0) AS week_fees
        FROM calendar c
        LEFT JOIN weekly_sparse ws
            ON  ws.week   = c.week
            AND ws.user   = c.user
            AND ws.symbol = c.symbol
    )
    SELECT
        week,
        user,
        symbol,
        week_fees,
        round(
            sum(week_fees) OVER (
                PARTITION BY user
                ORDER BY week
                ROWS BETWEEN 51 PRECEDING AND CURRENT ROW
            ),
            8
        ) AS annual_rolling_fees
    FROM weekly_dense
) s
WHERE (s.annual_rolling_fees > 0 OR s.week_fees > 0)
  
    AND s.week > (SELECT cutoff_week FROM cutoff)
  


UNION ALL


SELECT
    week,
    'holdings' AS stream_type,
    symbol,
    user,
    week_fees,
    annual_rolling_fees
FROM (
    WITH weekly_sparse AS (
        SELECT
            toStartOfWeek(date, 1) AS week,
            user,
            symbol,
            round(sum(fees), 8) AS week_fees
        FROM `dbt`.`int_revenue_holdings_fees_daily`
        WHERE date < toStartOfWeek(today(), 1)
          AND symbol = 'ZCHF'
          
            AND toStartOfWeek(date, 1) >= (SELECT cutoff_week FROM cutoff) - INTERVAL 52 WEEK
          
        GROUP BY week, user, symbol
    ),
    user_span AS (
        SELECT
            user,
            symbol,
            
              greatest(min(week),
                       (SELECT cutoff_week FROM cutoff) - INTERVAL 52 WEEK) AS first_week,
              toStartOfWeek(today(), 1) - INTERVAL 1 WEEK AS last_week
            
        FROM weekly_sparse
        GROUP BY 1, 2
    ),
    calendar AS (
        SELECT
            user,
            symbol,
            arrayJoin(
                arrayMap(
                    i -> toDate(addWeeks(first_week, i)),
                    range(toUInt32(dateDiff('week', first_week, last_week) + 1))
                )
            ) AS week
        FROM user_span
    ),
    weekly_dense AS (
        SELECT
            c.week,
            c.user,
            c.symbol,
            coalesce(ws.week_fees, 0) AS week_fees
        FROM calendar c
        LEFT JOIN weekly_sparse ws
            ON  ws.week   = c.week
            AND ws.user   = c.user
            AND ws.symbol = c.symbol
    )
    SELECT
        week,
        user,
        symbol,
        week_fees,
        round(
            sum(week_fees) OVER (
                PARTITION BY user
                ORDER BY week
                ROWS BETWEEN 51 PRECEDING AND CURRENT ROW
            ),
            8
        ) AS annual_rolling_fees
    FROM weekly_dense
) s
WHERE (s.annual_rolling_fees > 0 OR s.week_fees > 0)
  
    AND s.week > (SELECT cutoff_week FROM cutoff)
  


UNION ALL


SELECT
    week,
    'sdai' AS stream_type,
    symbol,
    user,
    week_fees,
    annual_rolling_fees
FROM (
    WITH weekly_sparse AS (
        SELECT
            toStartOfWeek(date, 1) AS week,
            user,
            symbol,
            round(sum(fees), 8) AS week_fees
        FROM `dbt`.`int_revenue_sdai_fees_daily`
        WHERE date < toStartOfWeek(today(), 1)
          AND symbol = 'sDAI'
          
            AND toStartOfWeek(date, 1) >= (SELECT cutoff_week FROM cutoff) - INTERVAL 52 WEEK
          
        GROUP BY week, user, symbol
    ),
    user_span AS (
        SELECT
            user,
            symbol,
            
              greatest(min(week),
                       (SELECT cutoff_week FROM cutoff) - INTERVAL 52 WEEK) AS first_week,
              toStartOfWeek(today(), 1) - INTERVAL 1 WEEK AS last_week
            
        FROM weekly_sparse
        GROUP BY 1, 2
    ),
    calendar AS (
        SELECT
            user,
            symbol,
            arrayJoin(
                arrayMap(
                    i -> toDate(addWeeks(first_week, i)),
                    range(toUInt32(dateDiff('week', first_week, last_week) + 1))
                )
            ) AS week
        FROM user_span
    ),
    weekly_dense AS (
        SELECT
            c.week,
            c.user,
            c.symbol,
            coalesce(ws.week_fees, 0) AS week_fees
        FROM calendar c
        LEFT JOIN weekly_sparse ws
            ON  ws.week   = c.week
            AND ws.user   = c.user
            AND ws.symbol = c.symbol
    )
    SELECT
        week,
        user,
        symbol,
        week_fees,
        round(
            sum(week_fees) OVER (
                PARTITION BY user
                ORDER BY week
                ROWS BETWEEN 51 PRECEDING AND CURRENT ROW
            ),
            8
        ) AS annual_rolling_fees
    FROM weekly_dense
) s
WHERE (s.annual_rolling_fees > 0 OR s.week_fees > 0)
  
    AND s.week > (SELECT cutoff_week FROM cutoff)
  


UNION ALL


SELECT
    week,
    'gpay' AS stream_type,
    symbol,
    user,
    week_fees,
    annual_rolling_fees
FROM (
    WITH weekly_sparse AS (
        SELECT
            toStartOfWeek(date, 1) AS week,
            user,
            symbol,
            round(sum(fees), 8) AS week_fees
        FROM `dbt`.`int_revenue_gpay_fees_daily`
        WHERE date < toStartOfWeek(today(), 1)
          AND symbol = 'EURe'
          
            AND toStartOfWeek(date, 1) >= (SELECT cutoff_week FROM cutoff) - INTERVAL 52 WEEK
          
        GROUP BY week, user, symbol
    ),
    user_span AS (
        SELECT
            user,
            symbol,
            
              greatest(min(week),
                       (SELECT cutoff_week FROM cutoff) - INTERVAL 52 WEEK) AS first_week,
              toStartOfWeek(today(), 1) - INTERVAL 1 WEEK AS last_week
            
        FROM weekly_sparse
        GROUP BY 1, 2
    ),
    calendar AS (
        SELECT
            user,
            symbol,
            arrayJoin(
                arrayMap(
                    i -> toDate(addWeeks(first_week, i)),
                    range(toUInt32(dateDiff('week', first_week, last_week) + 1))
                )
            ) AS week
        FROM user_span
    ),
    weekly_dense AS (
        SELECT
            c.week,
            c.user,
            c.symbol,
            coalesce(ws.week_fees, 0) AS week_fees
        FROM calendar c
        LEFT JOIN weekly_sparse ws
            ON  ws.week   = c.week
            AND ws.user   = c.user
            AND ws.symbol = c.symbol
    )
    SELECT
        week,
        user,
        symbol,
        week_fees,
        round(
            sum(week_fees) OVER (
                PARTITION BY user
                ORDER BY week
                ROWS BETWEEN 51 PRECEDING AND CURRENT ROW
            ),
            8
        ) AS annual_rolling_fees
    FROM weekly_dense
) s
WHERE (s.annual_rolling_fees > 0 OR s.week_fees > 0)
  
    AND s.week > (SELECT cutoff_week FROM cutoff)
  


UNION ALL


SELECT
    week,
    'gpay' AS stream_type,
    symbol,
    user,
    week_fees,
    annual_rolling_fees
FROM (
    WITH weekly_sparse AS (
        SELECT
            toStartOfWeek(date, 1) AS week,
            user,
            symbol,
            round(sum(fees), 8) AS week_fees
        FROM `dbt`.`int_revenue_gpay_fees_daily`
        WHERE date < toStartOfWeek(today(), 1)
          AND symbol = 'GBPe'
          
            AND toStartOfWeek(date, 1) >= (SELECT cutoff_week FROM cutoff) - INTERVAL 52 WEEK
          
        GROUP BY week, user, symbol
    ),
    user_span AS (
        SELECT
            user,
            symbol,
            
              greatest(min(week),
                       (SELECT cutoff_week FROM cutoff) - INTERVAL 52 WEEK) AS first_week,
              toStartOfWeek(today(), 1) - INTERVAL 1 WEEK AS last_week
            
        FROM weekly_sparse
        GROUP BY 1, 2
    ),
    calendar AS (
        SELECT
            user,
            symbol,
            arrayJoin(
                arrayMap(
                    i -> toDate(addWeeks(first_week, i)),
                    range(toUInt32(dateDiff('week', first_week, last_week) + 1))
                )
            ) AS week
        FROM user_span
    ),
    weekly_dense AS (
        SELECT
            c.week,
            c.user,
            c.symbol,
            coalesce(ws.week_fees, 0) AS week_fees
        FROM calendar c
        LEFT JOIN weekly_sparse ws
            ON  ws.week   = c.week
            AND ws.user   = c.user
            AND ws.symbol = c.symbol
    )
    SELECT
        week,
        user,
        symbol,
        week_fees,
        round(
            sum(week_fees) OVER (
                PARTITION BY user
                ORDER BY week
                ROWS BETWEEN 51 PRECEDING AND CURRENT ROW
            ),
            8
        ) AS annual_rolling_fees
    FROM weekly_dense
) s
WHERE (s.annual_rolling_fees > 0 OR s.week_fees > 0)
  
    AND s.week > (SELECT cutoff_week FROM cutoff)
  


UNION ALL


SELECT
    week,
    'gpay' AS stream_type,
    symbol,
    user,
    week_fees,
    annual_rolling_fees
FROM (
    WITH weekly_sparse AS (
        SELECT
            toStartOfWeek(date, 1) AS week,
            user,
            symbol,
            round(sum(fees), 8) AS week_fees
        FROM `dbt`.`int_revenue_gpay_fees_daily`
        WHERE date < toStartOfWeek(today(), 1)
          AND symbol = 'USDC.e'
          
            AND toStartOfWeek(date, 1) >= (SELECT cutoff_week FROM cutoff) - INTERVAL 52 WEEK
          
        GROUP BY week, user, symbol
    ),
    user_span AS (
        SELECT
            user,
            symbol,
            
              greatest(min(week),
                       (SELECT cutoff_week FROM cutoff) - INTERVAL 52 WEEK) AS first_week,
              toStartOfWeek(today(), 1) - INTERVAL 1 WEEK AS last_week
            
        FROM weekly_sparse
        GROUP BY 1, 2
    ),
    calendar AS (
        SELECT
            user,
            symbol,
            arrayJoin(
                arrayMap(
                    i -> toDate(addWeeks(first_week, i)),
                    range(toUInt32(dateDiff('week', first_week, last_week) + 1))
                )
            ) AS week
        FROM user_span
    ),
    weekly_dense AS (
        SELECT
            c.week,
            c.user,
            c.symbol,
            coalesce(ws.week_fees, 0) AS week_fees
        FROM calendar c
        LEFT JOIN weekly_sparse ws
            ON  ws.week   = c.week
            AND ws.user   = c.user
            AND ws.symbol = c.symbol
    )
    SELECT
        week,
        user,
        symbol,
        week_fees,
        round(
            sum(week_fees) OVER (
                PARTITION BY user
                ORDER BY week
                ROWS BETWEEN 51 PRECEDING AND CURRENT ROW
            ),
            8
        ) AS annual_rolling_fees
    FROM weekly_dense
) s
WHERE (s.annual_rolling_fees > 0 OR s.week_fees > 0)
  
    AND s.week > (SELECT cutoff_week FROM cutoff)
  
