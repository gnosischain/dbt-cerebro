{{
  config(
    materialized='view',
    tags=['staging','crawlers_data']
  )
}}

WITH src AS (
  SELECT
    lower(address)  AS address,
    label           AS label_raw,
    introduced_at
  FROM {{ source('crawlers_data','dune_labels') }}
),

ranked AS (
  SELECT
    address,
    label_raw,
    introduced_at,
    row_number() OVER (
      PARTITION BY address
      ORDER BY introduced_at DESC, label_raw DESC
    ) AS rn
  FROM src
),

latest AS (
  SELECT
    address,
    label_raw,
    introduced_at
  FROM ranked
  WHERE rn = 1
),

step1 AS (
  SELECT
    address,
    label_raw,
    introduced_at,
    trim(replaceRegexpAll(label_raw, '\\s*([:/|>])\\s*', '\\1')) AS s1
  FROM latest
),

step2 AS (
  SELECT
    address,
    label_raw,
    introduced_at,
    replaceRegexpAll(s1, '_0x[0-9a-fA-F]{40}$', '') AS s2a      -
  FROM step1
),
step2b AS (
  SELECT
    address,
    label_raw,
    introduced_at,
    replaceRegexpAll(s2a, '_0x[0-9a-fA-F]{1,}…[0-9a-fA-F]{1,}$', '') AS s2   
  FROM step2
),

step3 AS (
  SELECT
    address,
    label_raw,
    introduced_at,
    trim(extract(s2, '^([^:/|>]+)')) AS s3
  FROM step2b
),

step4 AS (
  SELECT
    address,
    label_raw,
    introduced_at,
    trim(replaceRegexpAll(s3, '\\s*[- ]?[Vv]\\d+(?:\\.\\d+)*\\b', '')) AS s4
  FROM step3
),

lowered AS (
  SELECT
    address,
    label_raw,
    introduced_at,
    s4,
    lowerUTF8(s4) AS s4_l
  FROM step4
),

bucketed AS (
  SELECT
    address,
    label_raw,
    introduced_at,
    multiIf(

      match(s4_l, '(^|[^a-z])balancer([^a-z]|$)'), 'Balancer',
      match(s4_l, '(^|[-_])gaug(e)?(\\b|_)'),      'Balancer',
      match(s4_l, '\\b\\d{1,3}%[a-z0-9._-]+'),     'Balancer',
      match(s4_l, '\\b(w?moo)[a-z0-9]*balancer'),  'Balancer',

      match(s4_l, '(^|[^a-z])curve([^a-z]|$)'),    'Curve',
      match(s4_l, '^(yv\\s*curve|yvcurve|y\\s*curve|ycurve)'), 'Curve',
      match(s4_l, '^curvefi\\b'),                  'Curve',

      match(s4_l, '\\buniswap\\b'),                'Uniswap',
      match(s4_l, '\\buni[- _]?v?3\\b'),           'Uniswap',
      match(s4_l, '\\buni[- _]?v?2\\b'),           'Uniswap',
      match(s4_l, '\\bnonfungiblepositionmanager\\b'), 'Uniswap',
      match(s4_l, '\\bpositions?\\s*nft\\b'),      'Uniswap',
      match(s4_l, '\\b(rcow|cow|moo\\w*)\\s*uniswap'), 'Uniswap',

      match(s4_l, 'sushi'),                        'Sushi',

      match(s4_l, '\\bswapr\\b'),                  'Swapr',
      match(s4_l, '^swaprv?3\\b'),                 'Swapr',
      match(s4_l, '\\bswpr\\b'),                   'Swapr',

      match(s4_l, '\\bcow\\s*swap\\b|\\bcow[_\\s-]?protocol\\b'), 'CowSwap',
/
      match(s4_l, '^aave\\b'),                     'Aave',
      match(s4_l, '\\baave\\s*v?2\\b|\\baave\\s*v?3\\b'), 'Aave',
      match(s4_l, '^aavepool\\b'),                 'Aave',

      s4
    ) AS s5
  FROM lowered
),

drop_roles AS (
  SELECT
    address,
    label_raw,
    introduced_at,
    s5,
    lowerUTF8(s5)                                                        AS s5_l,
    replaceRegexpAll(lowerUTF8(s5), '\\b(factory|router|vault|pool|implementation|proxy|token|bridge|aggregator|registry|controller|manager|oracle|staking|treasury|multisig|safe|gnosis\\s*safe|deployer|fee\\s*collector|minter|burner|timelock|governor|council|rewards?|distributor|airdrop)s?\\s*$', '') AS s5_l_stripped
  FROM bucketed
),

roles_applied AS (
  SELECT
    address,
    label_raw,
    introduced_at,
    left(s5, length(s5_l_stripped)) AS s6
  FROM drop_roles
),

finalize AS (
  SELECT
    address,
    label_raw,
    introduced_at,
    trim(replaceRegexpAll(replaceRegexpAll(s6, '\\s+', ' '), '[-_]+$', '')) AS s7
  FROM roles_applied
)

SELECT
  address,
  if(positionCaseInsensitive(s7, '0x') > 0, 'Others', s7) AS label,  
  label_raw,
  introduced_at
FROM finalize