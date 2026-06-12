{{
  config(
    materialized='table',
    tags=['production','execution','ubo','labels'],
    engine='MergeTree()',
    order_by=['address'],
    unique_key='address',
    on_cluster=None
  )
}}

-- Maps every labeled address to its UBO terminal classification.
-- Filters to the six sectors relevant for UBO coverage analysis only —
-- addresses in other sectors or with no label are absent and resolve to
-- is_terminal_ubo = NULL via LEFT JOIN in downstream consumers.
--
--   is_terminal_ubo = 1  — EOAs, Wallets & AA, Bridges, Payments
--   is_terminal_ubo = 0  — Lending & Yield, DEX

SELECT
    address,
    CASE
        WHEN sector IN ('EOAs', 'Wallets & AA', 'Bridges', 'Payments') THEN 1
        WHEN sector IN ('Lending & Yield', 'DEX')                      THEN 0
    END AS is_terminal_ubo
FROM {{ ref('int_crawlers_data_labels') }}
WHERE sector IN ('EOAs', 'Wallets & AA', 'Bridges', 'Payments', 'Lending & Yield', 'DEX')
