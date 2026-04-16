{{
    config(
        materialized='table',
        engine='MergeTree()',
        order_by='wrapper_address',
        settings={'allow_nullable_key': 1},
        tags=['production', 'execution', 'circles_v2', 'tokens']
    )
}}

{#
  Reference table mapping Circles V2 ERC-20 wrapper addresses to their
  on-chain token symbol.  Rebuilt daily so newly crawled dune_labels
  are picked up automatically.

  Symbol source: crawlers_data.dune_labels stores the on-chain symbol()
  in the label column as "{symbol}_0x{address}".  We extract everything
  before the first "_0x" occurrence.

  Coverage: ~92 % of wrappers have a dune_label.  For the remainder we
  fall back to CRC-{first 5 hex chars of wrapper address}.
#}

SELECT
    w.wrapper_address,
    w.avatar,
    w.circles_type,
    coalesce(
        nullIf(splitByString('_0x', dl.label)[1], ''),
        concat('CRC-', substring(w.wrapper_address, 3, 5))
    ) AS symbol
FROM {{ ref('int_execution_circles_v2_wrappers') }} w
LEFT JOIN {{ source('crawlers_data', 'dune_labels') }} dl
    ON dl.address = w.wrapper_address
