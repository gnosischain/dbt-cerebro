{{
  config(
    materialized='table',
    tags=['production','crawlers_data','labels']
  )
}}

WITH src AS (
  SELECT
    lower(address) AS address,
    project
  FROM {{ ref('stg_crawlers_data__dune_labels') }}
),

labeled AS (
  SELECT
    address,
    project,

    multiIf(

      match(project, '(?i)^Unknown$'),           'Unknown',
      match(project, '(?i)^EOA$'),               'EOAs',
      match(project, '(?i)^ERC20$'),             'ERC20 Tokens',

      match(project, '(?i)(uniswap|sushi|swapr|balancer|curve|honeyswap|levinswap|openocean|openoceanexchange|1inch|paraswap|cow\\s*swap|cowswap|gnosis\\s*protocol|xswap|symmetric(\\s*finance)?|elk\\s*finance|\\bdex\\b|aggregator|dex\\s*aggregator|zerion|kinetex\\s*xswap|baoswap)'),
      'DEX',

      match(project, '(?i)(\\baave\\b|spark(\\s*protocol)?|agave|compound|compoundadapter|\\bidle\\b|beefy|jarvis(\\s*network)?|gyroscope|qidao|powerpool|lending(manager)?|stakewise|aura\\s*finance|merkl)'),
      'Lending & Yield',

      match(project, '(?i)(\\bbridge\\b|omnibridge|hop(\\s*protocol)?|\\bbungee\\b|\\bacross\\b|connext|celer|stargate|lifi|li\\.?fi|\\bamb\\b|eth\\s*xdai\\s*amb|bsc\\s*xdai\\s*amb|\\brelay\\b|spokebridge|spokegateway|socket(\\b|\\s)|rango\\s*exchange|rubic(\\s*(onchain|crosschain))?|swaps?\\s*io|eywa|symbiosis|squid(\\s*spoke)?)'),
      'Bridges',

      match(project, '(?i)(layer\\s*zero|hyperlane|zk\\s*bridge|zkbridge|polyhedra|zk(light|client)|telepathy|messag(?:ing|e)|everclear|interop)'),
      'Messaging / Interop',

      match(project, '(?i)(safe(?:\\s*l2)?|gnosis\\s*safe|ambirewallet|biconomy|erc[- ]?4337|erc\\s*4337\\s*entry\\s*point|entry\\s*point|wethgateway|tokenbound|delegatecash|rhinestone|apex\\s*smart\\s*wallet|zeroexsettlerdeployersafemodule)'),
      'Wallets & AA',

      match(project, '(?i)(monerium(\\s*(iske|usde|blacklist))?|gnosis\\s*pay(\\s*(vip|spender|eiffel))?|\\bgpay\\b|request(\\s*network)?|\\busdc\\b|\\bsdai\\b|\\bxdai\\b|payments?|invoice|smart\\s*invoice|superfluid|sablier|swing\\s*xdai\\s*single\\s*chain)'),
      'Payments & Stablecoins',

      match(project, '(?i)(chainlink|tellor|pyth|\\boracle\\b|origin\\s*trail|origintrail|marketview|analytics|\\bdata\\b|\\bindex\\b|mu\\s*exchange\\s*pythoracle)'),
      'Oracles & Data',

      match(project, '(?i)(opensea|seaport|poap|nifty(ink|fair)?|\\bnft\\b|erc721|erc1155|foundation|eporio|marketplace|creator|mint|mech\\s*marketplace|ghost\\s*nft\\s*faucet|nfts2me|crypto\\s*stamp|nondescriptive\\s*1155)'),
      'NFTs & Marketplaces',

      match(project, '(?i)(dark\\s*forest|conquest\\.eth|mithraeum|\\bgame\\b|gaming)'),
      'Gaming',

      match(project, '(?i)(dao\\s*haus|daoh?a?us|daostack|\\bdao\\b|daosquare|plazadao|zodiac|snapshot|kleros(\\s*curate)?|reality\\s*eth|vocdoni|proof\\s*of\\s*humanity|sismo(\\s*(badges|roots|attestations\\s*registry))?|attestation|identity|omen|ran\\s*dao|circles|polkamarkets)'),
      'DAOs & Governance',

      match(project, '(?i)(tornado(?:\\.?\\s*cash)?|tornado\\s*cash\\s*nova|umbra|privacy|\\bmix\\b)'),
      'Privacy',

      match(project, '(?i)(autonolas|gnosis\\s*ai|autonomous|agent)'),
      'AI & Agents',

      match(project, '(?i)(real\\s*token|realtoken|real\\s*rmm|\\brmm\\b|emblem)'),
      'RWA & Tokenization',

      match(project, '(?i)(^infrastructure$|gelato|opengsn|obol|ankr|shutter|infra(structure)?|registry|deployer|factory|controller|manager|router|vault|pool|proxy|multisig|gnosis\\s*protocol|gnosis\\s*chain|xdai\\s*posdao|swarm|ethswarm|address\\s*tag\\s*registry|judicialassetfactory|hopr(\\s*(token|network|protocol))?)'),
      'Infrastructure & DevTools',

      'Others'
    ) AS sector
  FROM src
)

SELECT
  address,
  anyLast(project) AS project,
  anyLast(sector)  AS sector
FROM labeled
GROUP BY address