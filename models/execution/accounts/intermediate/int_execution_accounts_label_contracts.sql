{{
  config(
    materialized='table',
    engine='ReplacingMergeTree()',
    order_by='address',
    pre_hook=[
      "SET join_algorithm = 'grace_hash'",
      "SET max_bytes_in_join = 1000000000"
    ],
    post_hook=[
      "SET join_algorithm = 'default'",
      "SET max_bytes_in_join = 0"
    ],
    tags=['production','execution','accounts']
  )
}}

-- Protocol / token / infra contracts identified from Dune labels
-- (int_crawlers_data_labels), restricted to the non-user sectors. This
-- COMPLEMENTS int_execution_accounts_non_user_contracts, which is built from
-- trace sources (execution.contracts / code_diffs) + curated registries that
-- have gaps: label-only contracts such as the Balancer Vault
-- (0xba12...2c8, in neither trace source) leak into revenue as fake "users".
--
-- Kept as its own small model (~450k rows) rather than UNION-ed into the large,
-- un-batchable non_user_contracts table: the revenue stream models anti-join
-- BOTH sets, so this one can be built cheaply here while the 5.5M-row table is
-- left untouched. User sectors (EOAs, Wallets & AA, Payments) are intentionally
-- excluded; Safe proxies are exempted (anti-join below) so a mislabeled user
-- Safe still counts as a user.

WITH labeled AS (
    SELECT DISTINCT lower(address) AS address
    FROM {{ ref('int_crawlers_data_labels') }}
    WHERE sector IN (
      'DEX', 'Lending & Yield', 'Bridges', 'Messaging / Interop',
      'Oracles & Data', 'ERC20 Tokens', 'Stablecoins & Fiat Ramps',
      'NFTs & Marketplaces', 'Infrastructure & DevTools', 'RWA & Tokenization'
    )
)

SELECT l.address AS address
FROM labeled l
LEFT ANTI JOIN (
    SELECT DISTINCT lower(address) AS address FROM {{ ref('contracts_safe_registry') }}
) sr ON sr.address = l.address
