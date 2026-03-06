{{
  config(
    materialized='view',
    tags=['production','staging','crawlers_data']
  )
}}


WITH latest AS (
  SELECT
    lower(address) AS address,
    (label, introduced_at) AS agg
  FROM {{ source('crawlers_data','dune_labels') }}
),

clean AS (
  SELECT
    address,
    tupleElement(agg, 1) AS label_raw,
    tupleElement(agg, 2) AS introduced_at,

    trim(replaceRegexpAll(tupleElement(agg, 1), '\\s*([:/|>])\\s*', '\\1'))                           AS s1,

    replaceRegexpAll(
      replaceRegexpAll(s1, '_0x[0-9a-fA-F]{40}$', ''),
      '_0x[0-9a-fA-F]{1,}…[0-9a-fA-F]{1,}$',
      ''
    )                                                                                                AS s2,

    trim(extract(s2, '^([^:/|>]+)'))                                                                 AS s3,

    trim(replaceRegexpAll(s3, '(?:\\s*[-_ ]?[Vv]\\d+(?:[._-]\\d+)*)\\b', ''))                        AS s4,

    lowerUTF8(s4)                                                                                    AS s4_l,

    (
      match(s1, '_0x[0-9a-fA-F]{40}$')
      OR match(s1, '_0x[0-9a-fA-F]{1,}…[0-9a-fA-F]{1,}$')
    )                                                                                                AS looks_like_token_tail
  FROM latest
),

wl AS (
  SELECT
    lower(address) AS address,
    symbol
  FROM {{ ref('tokens_whitelist') }}
),

bucketed AS (
 
  SELECT
    c.address,
    c.label_raw,
    c.introduced_at,
    c.s4,
    c.s4_l,
    c.looks_like_token_tail,
    w.symbol AS wl_symbol,

    coalesce(
      if(c.looks_like_token_tail,
         if(w.symbol IS NOT NULL, w.symbol, 'ERC20'),
         NULL
      ),

      multiIf(
        match(c.s4_l, '^(realtoken|realtokens)\\b'),              'REALTOKEN',
        match(lowerUTF8(c.label_raw), '(^|[^a-z0-9])gnosis[\\s_-]*safe(?:l2)?([^a-z0-9]|$)') OR match(c.s4_l, '^(safe(?:l2)?)\\b'), 'Safe',

        match(c.s4_l, '(^|[^a-z])balancer([^a-z]|$)'),            'Balancer',
        match(c.s4_l, '(^|[-_])gaug(e)?(\\b|_)'),                 'Balancer',
        match(c.s4_l, '\\b\\d{1,3}%[a-z0-9._-]+'),                'Balancer',
        match(c.s4_l, '\\b(w?moo)[a-z0-9]*balancer'),             'Balancer',

        match(c.s4_l, '(^|[^a-z])curve([^a-z]|$)'),               'Curve',
        match(c.s4_l, '^(yv\\s*curve|yvcurve|y\\s*curve|ycurve)'), 'Curve',
        match(c.s4_l, '^curvefi\\b'),                             'Curve',

        match(c.s4_l, '\\buniswap\\b'),                           'Uniswap',
        match(c.s4_l, '\\buni[- _]?v?3\\b'),                      'Uniswap',
        match(c.s4_l, '\\buni[- _]?v?2\\b'),                      'Uniswap',
        match(c.s4_l, '\\bnonfungiblepositionmanager\\b'),        'Uniswap',
        match(c.s4_l, '\\bpositions?\\s*nft\\b'),                 'Uniswap',
        match(c.s4_l, '\\b(rcow|cow|moo\\w*)\\s*uniswap'),        'Uniswap',

        match(c.s4_l, 'sushi'),                                   'Sushi',

        match(c.s4_l, '\\bswapr\\b'),                             'Swapr',
        match(c.s4_l, '^swaprv?3\\b'),                            'Swapr',
        match(c.s4_l, '\\bswpr\\b'),                              'Swapr',

        match(c.s4_l, '\\bcow\\s*swap\\b|\\bcow[_\\s-]?protocol\\b|^b_cow_amm\\b'), 'CowSwap',
        match(c.s4_l, '^aave\\b'),                                'Aave',
        match(c.s4_l, '\\baave\\s*v?2\\b|\\baave\\s*v?3\\b'),     'Aave',
        match(c.s4_l, '^aavepool\\b'),                            'Aave',

        c.s4
      )
    ) AS s5
  FROM clean c
  LEFT JOIN wl w
    ON c.address = w.address
),

tidy AS (
  SELECT
    address,
    label_raw,
    introduced_at,
    trim(
      replaceRegexpAll(
        replaceRegexpAll(
          replaceRegexpAll(
            left(s5, length(
              replaceRegexpAll(
                lowerUTF8(s5),
                '\\b(factory|router|vault|pool|implementation|proxy|token|bridge|aggregator|registry|controller|manager|oracle|staking|treasury|multisig|gnosis\\s*safe|deployer|fee\\s*collector|minter|burner|timelock|governor|council|rewards?|distributor|airdrop)s?\\s*$',
                ''
              )
            )),
            '\\s*\\([^)]*\\)\\s*$', ''
          ),
          '\\?+$', ''
        ),
        '[_\\s-]+', ' '
      )
    ) AS s7
  FROM bucketed
),

canon AS (
  SELECT
    address,
    label_raw,
    introduced_at,
    s7,
    multiIf(
      match(lowerUTF8(s7), '^unlock(\\s*protocol)?$'),                        'Unlock Protocol',
      match(lowerUTF8(s7), '^(eth\\s*swarm|ethereum\\s*swarm|swarm)$'),       'Swarm',
      match(lowerUTF8(s7), '^symmetric(\\s*finance)?$'),                      'Symmetric',
      match(lowerUTF8(s7), '^elk(\\s*finance)?$'),                            'Elk Finance',
      match(lowerUTF8(s7), '^monerium(\\s*(usde|iske|blacklist))?$'),         'Monerium',
      match(lowerUTF8(s7), '^gnosis\\s*pay(\\s*(vip|spender|eiffel))?$|^gnosispay$'), 'Gnosis Pay',
      match(lowerUTF8(s7), '^hopr(\\s*(protocol|network|token|boost\\s*nft))?$'),    'HOPR',
      match(lowerUTF8(s7), '^kleros(\\s*curate)?$'),                          'Kleros',
      match(lowerUTF8(s7), '^sismo(\\s*(badges|roots))?$'),                   'Sismo',
      match(lowerUTF8(s7), '^request(\\s*network)?$'),                        'Request Network',
      match(lowerUTF8(s7), '^event\\s*sbt(\\s*\\(esbt\\)\\s*test)?$'),        'EventSBT',
      match(lowerUTF8(s7), '^polkamarkets(\\s*aa)?$'),                        'Polkamarkets',
      match(lowerUTF8(s7), '^everclear.*$'),                                  'Everclear',
      match(lowerUTF8(s7), '^merkly(\\s*farmer)?$'),                          'Merkly',
      match(lowerUTF8(s7), '^zerion(\\s*premium\\s*purchaser\\s*l2)?$'),      'Zerion',
      match(lowerUTF8(s7), '^aura(\\s*finance)?$'),                           'Aura Finance',
      match(lowerUTF8(s7), '^open\\s*ocean(exchange)?$|^openoceanexchange$'), 'OpenOcean',
      match(lowerUTF8(s7), '^(amb|.*\\sxdai\\s*amb)$'),                       'AMB',
      match(lowerUTF8(s7), '^cow\\s*swap$'),                                  'CowSwap',
      match(lowerUTF8(s7), '^(rmm|real\\s*rmm|rmm\\s*ecosystem\\s*reserve)$'), 'RMM',
      match(lowerUTF8(s7), '^erc\\s*-?\\s*4337(\\s*entry\\s*point)?$'), 'ERC-4337 Entry Point',
      match(lowerUTF8(s7), '^(aa\\s*entrypoint|erc-?4337\\s*entry\\s*point|entrypointsimulations|pimlicoentrypointsimulations)$'), 'ERC-4337 Entry Point',
      match(lowerUTF8(s7), '^(uni|uni\\s*v?3\\s*swaprouter02)$'),                                      'Uniswap',
      match(lowerUTF8(s7), '^(oneinch)$'),                                                             '1inch',
      match(lowerUTF8(s7), '^layerzero$'),                                                             'LayerZero',
      match(lowerUTF8(s7), '^(cowswap|b_cow_amm)$'),                                                   'CowSwap',
      match(lowerUTF8(s7), '^angle(\\s+ageur)?$'),                                                     'Angle',
      match(lowerUTF8(s7), '^sablier(\\s+.*)?$|^sablier\\s*flow\\b|^sablier\\s*lockup\\b'),            'Sablier',
      match(lowerUTF8(s7), '^hats[_\\s-]?protocol'),                                                   'Hats Protocol',
      match(lowerUTF8(s7), '^seaport\\??$'),                                                           'Seaport',
      match(lowerUTF8(s7), '^poap(\\s+top)?$'),                                                        'POAP',
      match(lowerUTF8(s7), '^merkly(\\s+onft)?$'),                                                     'Merkly',
      match(lowerUTF8(s7), '^circles(\\s*ubi)?$'),                                                     'Circles',
      match(lowerUTF8(s7), '^(hop|hop\\s*protocol)$'),                                                 'Hop Protocol',
      match(lowerUTF8(s7), '^opensea$'),                                                               'OpenSea',
      match(lowerUTF8(s7), '^paraswap$'),                                                              'ParaSwap',
      match(lowerUTF8(s7), '^realt(oken)?(\\s*money\\s*market)?$|^realtoken\\s*dao$|^realtyam$|^real_rmm$'), 'REALTOKEN',
      match(lowerUTF8(s7), '^nulladdress$|^burn\\s*address$'),                                         'Null/Burn',
      match(lowerUTF8(s7), '^eoa$'),                                                                   'EOA',
      match(lowerUTF8(s7), '^proxyadmin$|^transparentupgradeableproxy$|^upgradeablecommunitytoken$|^contractaddressfeehelper$|^controllermodule$'), 'Infrastructure',
      match(lowerUTF8(s7), '^unnamed$|^oracle\\?$|^dex\\s*aggregator\\?$|^\\?$'),                      'Unknown',
      s7
    ) AS project_canon
  FROM tidy
)

SELECT
  address,
  if(
    lengthUTF8(project_canon) = 0
    OR positionCaseInsensitive(project_canon, '0x') > 0
    OR match(label_raw, '_0x[0-9a-fA-F]{40}$')
    OR match(label_raw, '_0x[0-9a-fA-F]{1,}…[0-9a-fA-F]{1,}$'),
    'ERC20',
    project_canon
  ) AS project,
  label_raw AS project_raw,
  introduced_at
FROM canon