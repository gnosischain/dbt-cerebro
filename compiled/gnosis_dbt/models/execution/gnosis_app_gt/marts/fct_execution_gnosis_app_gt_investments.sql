

-- METRI auto-invest accounts (product='metri'). Metri (app.metri.xyz) is the
-- LEGACY Gnosis App, so these owners ARE (legacy) Gnosis App users — the current
-- heuristic misses ~98% of them (it only sees the current app.gnosis.io), which
-- is exactly the legacy population the GT suite recovers. NOT a Gnosis Pay
-- feature. Owner-keyed aggregate (account_address is a different address space
-- from owner).
SELECT
    'metri'                         AS product,
    is_active,
    count()                         AS n_accounts,
    uniqExact(owner)                AS n_owners,
    uniqExact(investment_token)     AS n_tokens
FROM `dbt`.`stg_envio_ga__investment_accounts`
GROUP BY is_active