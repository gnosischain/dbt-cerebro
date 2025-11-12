

WITH base AS (
  SELECT
    date,
    source_chain              AS source_chain,
    dest_chain                AS dest_chain,
    bridge                    AS bridge,
    token                     AS token,
    toFloat64(volume_usd)     AS volume_usd
  FROM `dbt`.`int_bridges_flows_daily`
  WHERE date < today()  
),

out_left AS (
  SELECT
    b.date,
    'out'         AS direction,
    'left'        AS side,
    'gnosis'      AS source,
    b.bridge      AS target,
    b.token,
    sum(b.volume_usd) AS value
  FROM base b
  WHERE b.source_chain = 'gnosis'
  GROUP BY b.date, direction, side, source, target, b.token
),
out_right AS (
  SELECT
    b.date,
    'out'         AS direction,
    'right'       AS side,
    b.bridge      AS source,
    b.dest_chain  AS target,   
    b.token,
    sum(b.volume_usd) AS value
  FROM base b
  WHERE b.source_chain = 'gnosis'
    AND b.dest_chain  != 'gnosis'
  GROUP BY b.date, direction, side, source, target, b.token
),

in_left AS (
  SELECT
    b.date,
    'in'          AS direction,
    'left'        AS side,
    b.source_chain AS source,  
    b.bridge       AS target,
    b.token,
    sum(b.volume_usd) AS value
  FROM base b
  WHERE b.dest_chain = 'gnosis'
    AND b.source_chain != 'gnosis'
  GROUP BY b.date, direction, side, source, target, b.token
),
in_right AS (
  SELECT
    b.date,
    'in'          AS direction,
    'right'       AS side,
    b.bridge      AS source,
    'gnosis'      AS target,
    b.token,
    sum(b.volume_usd) AS value
  FROM base b
  WHERE b.dest_chain = 'gnosis'
  GROUP BY b.date, direction, side, source, target, b.token
)

SELECT * FROM out_left
UNION ALL SELECT * FROM out_right
UNION ALL SELECT * FROM in_left
UNION ALL SELECT * FROM in_right