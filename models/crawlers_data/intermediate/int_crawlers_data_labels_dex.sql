{{
  config(
    materialized = 'table',
    tags = ['production','crawlers_data','labels'],
    unique_key = 'address',
    engine = 'MergeTree()',
    order_by = ['address'],
    on_cluster = None
  )
}}

SELECT
  address,
  project
FROM {{ ref('int_crawlers_data_labels') }}
WHERE sector NOT IN ('EOAs', 'ERC20 Tokens', 'Wallets & AA', 'Payments')
