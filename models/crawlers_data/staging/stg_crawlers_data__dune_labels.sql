{{
  config(
    materialized='view',
    tags=['staging','crawlers_data']
  )
}}

WITH latest AS (
  SELECT
    lower(address) AS address,
    argMax( (label, introduced_at), (introduced_at, label) ) AS agg
  FROM {{ source('crawlers_data','dune_labels') }}
  GROUP BY address
),

norm AS (
  SELECT
    address,
    tupleElement(agg, 1) AS label_raw,
    tupleElement(agg, 2) AS introduced_at,

    trim(replaceRegexpAll(tupleElement(agg, 1), '\\s*([:/|>])\\s*', '\\1'))                                                  AS s1,
    replaceRegexpAll(replaceRegexpAll(s1, '_0x[0-9a-fA-F]{40}$', ''), '_0x[0-9a-fA-F]{1,}…[0-9a-fA-F]{1,}$', '')             AS s2,
    trim(extract(s2, '^([^:/|>]+)'))                                                                                          AS s3,
    trim(replaceRegexpAll(s3, '\\s*[- ]?[Vv]\\d+(?:\\.\\d+)*\\b', ''))                                                        AS s4,
    lowerUTF8(s4)                                                                                                             AS s4_l,

    multiIf(
      match(s4_l, '^(gnosis[\\s_-]*safe|safe(?:l2)?)\\b'), 'Safe',

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
      match(s4_l, '^aave\\b'),                     'Aave',
      match(s4_l, '\\baave\\s*v?2\\b|\\baave\\s*v?3\\b'), 'Aave',
      match(s4_l, '^aavepool\\b'),                 'Aave',

      s4
    )                                                                                                                         AS s5,

    lowerUTF8(s5)                                                                                                             AS s5_l,
    replaceRegexpAll(
      s5_l,
      '\\b(factory|router|vault|pool|implementation|proxy|token|bridge|aggregator|registry|controller|manager|oracle|staking|treasury|multisig|gnosis\\s*safe|deployer|fee\\s*collector|minter|burner|timelock|governor|council|rewards?|distributor|airdrop)s?\\s*$',
      ''
    )                                                                                                                         AS s5_l_stripped,
    left(s5, length(s5_l_stripped))                                                                                           AS s6,
    trim(replaceRegexpAll(replaceRegexpAll(s6, '\\s+', ' '), '[-_]+$', ''))                                                   AS s7
  FROM latest
)

SELECT
  address,
  if(lengthUTF8(s7)=0 OR positionCaseInsensitive(s7, '0x') > 0, 'Others', s7) AS project,
  label_raw AS project_raw,
  introduced_at
FROM norm
