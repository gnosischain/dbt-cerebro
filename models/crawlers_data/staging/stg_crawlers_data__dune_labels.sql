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

    trim(replaceRegexpAll(tupleElement(agg, 1), '\\s*([:/|>])\\s*', '\\1')) AS s1,

    replaceRegexpAll(
      replaceRegexpAll(s1, '_0x[0-9a-fA-F]{40}$', ''),
      '_0x[0-9a-fA-F]{1,}…[0-9a-fA-F]{1,}$',
      ''
    ) AS s2,

    trim(extract(s2, '^([^:/|>]+)')) AS s3,

    trim(
      replaceRegexpAll(
        s3,
        '(?:\\s*[-_ ]?[Vv]\\d+(?:[._-]\\d+)*)\\b',
        ''
      )
    ) AS s4,

    lowerUTF8(s4) AS s4_l,

    ( match(s1, '_0x[0-9a-fA-F]{40}$')
      OR match(s1, '_0x[0-9a-fA-F]{1,}…[0-9a-fA-F]{1,}$')
    ) AS looks_like_token_tail,

    trim(
      replaceRegexpOne(
        replaceRegexpOne(s1, '_0x[0-9a-fA-F]{40}$', ''),
        '_0x[0-9a-fA-F]{1,}…[0-9a-fA-F]{1,}$',
        ''
      )
    ) AS token_prefix_raw
  FROM latest
),

norm2 AS (
  SELECT
    *,
    trim(replaceRegexpAll(token_prefix_raw, '\\s+', ' ')) AS token_prefix,
    lowerUTF8(trim(replaceRegexpAll(token_prefix_raw, '\\s+', ' '))) AS token_prefix_l
  FROM norm
),

wl AS (
  SELECT lowerUTF8(token_label) AS token_label
  FROM {{ ref('tokens_whitelist') }}
),

flags AS (
  SELECT
    n.*,
    (w.token_label IS NOT NULL) AS token_whitelisted,

    (
      match(token_prefix_l, '\\b(https?://|bit\\.ly|t\\.ly)') OR
      match(token_prefix_l, '\\[[^\\]]+\\]') OR
      match(token_prefix_l, '(^|\\s)(claim|airdrop|reward|bonus|metawin|vanityeth|takeusdmoney)\\b') OR
      match(token_prefix_l, '([a-z0-9-]+\\.)+(cfd|lol|lat|biz|one|farm|icu|cc|org|com|net|xyz|top|link|site|info|online|store|click)\\b') OR
      match(token_prefix_raw, '^\\s*\\$') OR
      match(token_prefix_l, '^[^a-z0-9]+$')
    ) AS looks_like_spam,

    match(token_prefix_l, '^(nf)')                   AS token_is_nf_family,
    match(token_prefix_l, '^(realtoken|realtokens)') AS token_is_realtoken_family
  FROM norm2 n
  LEFT JOIN wl w
    ON n.token_prefix_l = w.token_label
),

bucketed AS (
  SELECT
    address,
    label_raw,
    introduced_at,
    s4,
    s4_l,
    looks_like_token_tail,
    token_prefix,
    token_prefix_l,
    token_whitelisted,
    looks_like_spam,
    token_is_nf_family,
    token_is_realtoken_family,

    coalesce(
      if(looks_like_token_tail AND looks_like_spam, 'ERC20', NULL),

      if(looks_like_token_tail AND token_is_nf_family, 'ERC20', NULL),

      if(looks_like_token_tail AND token_is_realtoken_family, 'REALTOKEN', NULL),

      if(looks_like_token_tail,
         if(token_whitelisted, token_prefix, 'ERC20'),
         NULL
      ),

      multiIf(
        match(s4_l, '^(realtoken|realtokens)\\b'),   'REALTOKEN',
        match(s4_l, '^(gnosis[\\s_-]*safe|safe(?:l2)?)\\b'), 'Safe',

        match(s4_l, '(^|[^a-z])balancer([^a-z]|$)'), 'Balancer' ,
        match(s4_l, '(^|[-_])gaug(e)?(\\b|_)'),      'Balancer' ,
        match(s4_l, '\\b\\d{1,3}%[a-z0-9._-]+'),     'Balancer' ,
        match(s4_l, '\\b(w?moo)[a-z0-9]*balancer'),  'Balancer' ,

        match(s4_l, '(^|[^a-z])curve([^a-z]|$)'),    'Curve'    ,
        match(s4_l, '^(yv\\s*curve|yvcurve|y\\s*curve|ycurve)'), 'Curve',
        match(s4_l, '^curvefi\\b'),                  'Curve'    ,

        match(s4_l, '\\buniswap\\b'),                'Uniswap'  ,
        match(s4_l, '\\buni[- _]?v?3\\b'),           'Uniswap'  ,
        match(s4_l, '\\buni[- _]?v?2\\b'),           'Uniswap'  ,
        match(s4_l, '\\bnonfungiblepositionmanager\\b'), 'Uniswap',
        match(s4_l, '\\bpositions?\\s*nft\\b'),      'Uniswap'  ,
        match(s4_l, '\\b(rcow|cow|moo\\w*)\\s*uniswap'), 'Uniswap',

        match(s4_l, 'sushi'),                        'Sushi'    ,

        match(s4_l, '\\bswapr\\b'),                  'Swapr'    ,
        match(s4_l, '^swaprv?3\\b'),                 'Swapr'    ,
        match(s4_l, '\\bswpr\\b'),                   'Swapr'    ,

        match(s4_l, '\\bcow\\s*swap\\b|\\bcow[_\\s-]?protocol\\b|^b_cow_amm\\b'), 'CowSwap',
        match(s4_l, '^aave\\b'),                     'Aave'     ,
        match(s4_l, '\\baave\\s*v?2\\b|\\baave\\s*v?3\\b'), 'Aave',
        match(s4_l, '^aavepool\\b'),                 'Aave'     ,

        s4
      )
    ) AS s5
  FROM flags
),

drop_roles AS (
  SELECT
    address,
    label_raw,
    introduced_at,
    s5,
    lowerUTF8(s5) AS s5_l,
    replaceRegexpAll(
      lowerUTF8(s5),
      '\\b(factory|router|vault|pool|implementation|proxy|token|bridge|aggregator|registry|controller|manager|oracle|staking|treasury|multisig|gnosis\\s*safe|deployer|fee\\s*collector|minter|burner|timelock|governor|council|rewards?|distributor|airdrop)s?\\s*$',
      ''
    ) AS s5_l_stripped
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

postclean AS (
  SELECT
    address,
    label_raw,
    introduced_at,
    trim(
      replaceRegexpAll(
        replaceRegexpAll(
          replaceRegexpAll(s6, '\\s*\\([^)]*\\)\\s*$', ''),  
          '\\?+$', ''                                         
        ),
        '[_\\s-]+', ' '                                      
      )
    ) AS s7
  FROM roles_applied
),

canon AS (
  SELECT
    address,
    label_raw,
    introduced_at,
    s7,
    lowerUTF8(s7) AS s7_l,

    multiIf(
      match(lowerUTF8(s7), '^(aa\\s*entrypoint|erc-?4337\\s*entry\\s*point|entrypointsimulations|pimlicoentrypointsimulations)$'), 'ERC-4337 Entry Point',

      match(lowerUTF8(s7), '^(uni|uni\\s*v?3\\s*swaprouter02)$'), 'Uniswap',

      match(lowerUTF8(s7), '^(oneinch)$'), '1inch',

      match(lowerUTF8(s7), '^layerzero$'), 'LayerZero',

      match(lowerUTF8(s7), '^(cowswap|b_cow_amm)$'), 'CowSwap',

      match(lowerUTF8(s7), '^angle(\\s+ageur)?$'), 'Angle',

      match(lowerUTF8(s7), '^sablier(\\s+.*)?$|^sablier\\s*flow\\b|^sablier\\s*lockup\\b'), 'Sablier',

      match(lowerUTF8(s7), '^hats[_\\s-]?protocol'), 'Hats Protocol',

      match(lowerUTF8(s7), '^seaport\\??$'), 'Seaport',

      match(lowerUTF8(s7), '^poap(\\s+top)?$'), 'POAP',

      match(lowerUTF8(s7), '^merkly(\\s+onft)?$'), 'Merkly',

      match(lowerUTF8(s7), '^circles(\\s*ubi)?$'), 'Circles',

      -- Hop (bridge) vs HOPR (privacy); keep HOPR as-is, map plain HOP to Hop Protocol
      match(lowerUTF8(s7), '^(hop|hop\\s*protocol)$'), 'Hop Protocol',

      match(lowerUTF8(s7), '^opensea$'), 'OpenSea',

      match(lowerUTF8(s7), '^paraswap$'), 'ParaSwap',

      match(lowerUTF8(s7), '^realt(oken)?(\\s*money\\s*market)?$|^realtoken\\s*dao$|^realtyam$|^real_rmm$'), 'REALTOKEN',

      match(lowerUTF8(s7), '^nulladdress$|^burn\\s*address$'), 'Null/Burn',
      match(lowerUTF8(s7), '^eoa$'), 'EOA',

      match(lowerUTF8(s7), '^proxyadmin$|^transparentupgradeableproxy$|^upgradeablecommunitytoken$|^contractaddressfeehelper$|^controllermodule$'), 'Infrastructure',
      match(lowerUTF8(s7), '^unnamed$|^oracle\\?$|^dex\\s*aggregator\\?$|^\\?$'), 'Unknown',

      s7
    ) AS project_canon
  FROM postclean
),

finalize AS (
  SELECT
    address,
    label_raw,
    introduced_at,
    project_canon AS s8
  FROM canon
)

SELECT
  address,
  if(
    lengthUTF8(s8) = 0
    OR positionCaseInsensitive(s8, '0x') > 0
    OR match(label_raw, '_0x[0-9a-fA-F]{40}$')
    OR match(label_raw, '_0x[0-9a-fA-F]{1,}…[0-9a-fA-F]{1,}$'),
    'ERC20',
    s8
  ) AS project,
  label_raw  AS project_raw,
  introduced_at
FROM finalize