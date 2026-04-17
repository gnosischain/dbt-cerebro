{{
    config(
        materialized='table',
        engine='ReplacingMergeTree()',
        order_by='(address)',
        tags=["execution", "shared", "identity", "graph_explorer"],
        query_settings={
            'max_bytes_before_external_group_by': '2000000000',
            'max_bytes_before_external_sort': '2000000000'
        }
    )
}}

-- Identity-role pivot: one row per address, with boolean flags for every
-- role the address plays.
--
-- Implementation:
--   1. Each per-source CTE aggregates to ONE ROW PER UNIQUE ADDRESS with
--      its flag and any metadata already collapsed via any()/max().
--      This keeps huge upstreams (dex_liquidity_events at billions of
--      rows, transfers at hundreds of millions) compressed to millions
--      of unique addresses BEFORE we union.
--   2. UNION ALL + one final GROUP BY address coalesces across sources.
--
-- Memory stays bounded by |unique addresses| × 11 source blocks, not by
-- upstream row counts.

WITH
safes AS (
    SELECT lower(address) AS address
    FROM {{ ref('contracts_safe_registry') }}
    WHERE address IS NOT NULL
    GROUP BY 1
),
gpay AS (
    SELECT lower(address) AS address
    FROM {{ ref('int_execution_gpay_wallets') }}
    WHERE address IS NOT NULL
    GROUP BY 1
),
ga_users AS (
    SELECT
        lower(first_ga_owner_address) AS address,
        any(ifNull(pay_wallet, '')) AS controls_gpay_wallet
    FROM {{ ref('int_execution_gnosis_app_gpay_wallets') }}
    WHERE first_ga_owner_address IS NOT NULL
      AND is_currently_ga_owned = 1
    GROUP BY 1
),
circles AS (
    SELECT
        lower(avatar) AS address,
        any(ifNull(avatar_type, '')) AS circles_avatar_type
    FROM {{ ref('int_execution_circles_v2_avatars') }}
    WHERE avatar IS NOT NULL
    GROUP BY 1
),
wrappers AS (
    SELECT lower(wrapper_address) AS address
    FROM {{ ref('int_execution_circles_v2_wrappers') }}
    WHERE wrapper_address IS NOT NULL
    GROUP BY 1
),
safe_owners AS (
    SELECT lower(owner) AS address
    FROM {{ ref('int_execution_safes_current_owners') }}
    WHERE owner IS NOT NULL
    GROUP BY 1
),
-- One scan of pools_dex_liquidity_events emits both lp-provider and pool
-- rows via arrayJoin — halves memory + IO vs two separate aggregations.
dex_addresses AS (
    SELECT
        tup.1 AS role,
        tup.2 AS address,
        any(protocol) AS pool_protocol
    FROM {{ ref('int_execution_pools_dex_liquidity_events') }}
    ARRAY JOIN [('lp', lower(ifNull(provider, ''))),
                ('pool', lower(ifNull(pool_address, '')))] AS tup
    WHERE tup.2 != ''
    GROUP BY role, address
),
lps AS (
    SELECT address, pool_protocol FROM dex_addresses WHERE role = 'lp'
),
pools AS (
    SELECT address FROM dex_addresses WHERE role = 'pool'
),
lenders AS (
    SELECT lower(user_address) AS address
    FROM {{ ref('fct_execution_yields_user_lending_positions_latest') }}
    WHERE user_address IS NOT NULL
    GROUP BY 1
),
validators AS (
    SELECT withdrawal_address AS address
    FROM {{ ref('int_consensus_validators_withdrawal_addresses') }}
    WHERE withdrawal_address IS NOT NULL
    GROUP BY 1
),
labels AS (
    SELECT
        lower(address) AS address,
        any(ifNull(project, '')) AS dune_project
    FROM {{ ref('int_crawlers_data_labels') }}
    WHERE address IS NOT NULL
    GROUP BY 1
),

all_rows AS (
    SELECT address, 1 AS is_safe, 0 AS is_gpay_wallet, 0 AS is_ga_user,
           CAST('' AS String) AS controls_gpay_wallet,
           0 AS is_circles_avatar, CAST('' AS String) AS circles_avatar_type,
           0 AS is_circles_wrapper, 0 AS is_safe_owner,
           0 AS is_lp_provider, CAST('' AS String) AS pool_protocol,
           0 AS is_pool, 0 AS is_lending_user, 0 AS is_validator_depositor,
           0 AS has_dune_label, CAST('' AS String) AS dune_project
    FROM safes

    UNION ALL
    SELECT address, 0, 1, 0, '', 0, '', 0, 0, 0, '', 0, 0, 0, 0, ''
    FROM gpay

    UNION ALL
    SELECT address, 0, 0, 1, controls_gpay_wallet, 0, '', 0, 0, 0, '', 0, 0, 0, 0, ''
    FROM ga_users

    UNION ALL
    SELECT address, 0, 0, 0, '', 1, circles_avatar_type, 0, 0, 0, '', 0, 0, 0, 0, ''
    FROM circles

    UNION ALL
    SELECT address, 0, 0, 0, '', 0, '', 1, 0, 0, '', 0, 0, 0, 0, ''
    FROM wrappers

    UNION ALL
    SELECT address, 0, 0, 0, '', 0, '', 0, 1, 0, '', 0, 0, 0, 0, ''
    FROM safe_owners

    UNION ALL
    SELECT address, 0, 0, 0, '', 0, '', 0, 0, 1, pool_protocol, 0, 0, 0, 0, ''
    FROM lps

    UNION ALL
    SELECT address, 0, 0, 0, '', 0, '', 0, 0, 0, '', 1, 0, 0, 0, ''
    FROM pools

    UNION ALL
    SELECT address, 0, 0, 0, '', 0, '', 0, 0, 0, '', 0, 1, 0, 0, ''
    FROM lenders

    UNION ALL
    SELECT address, 0, 0, 0, '', 0, '', 0, 0, 0, '', 0, 0, 1, 0, ''
    FROM validators

    UNION ALL
    SELECT address, 0, 0, 0, '', 0, '', 0, 0, 0, '', 0, 0, 0, 1, dune_project
    FROM labels
)

SELECT
    CAST(address AS String) AS address,
    max(is_safe) AS is_safe,
    max(is_gpay_wallet) AS is_gpay_wallet,
    max(is_ga_user) AS is_ga_user,
    anyIf(controls_gpay_wallet, controls_gpay_wallet != '') AS controls_gpay_wallet,
    max(is_circles_avatar) AS is_circles_avatar,
    anyIf(circles_avatar_type, circles_avatar_type != '') AS circles_avatar_type,
    max(is_circles_wrapper) AS is_circles_wrapper,
    max(is_safe_owner) AS is_safe_owner,
    max(is_lp_provider) AS is_lp_provider,
    anyIf(pool_protocol, pool_protocol != '') AS pool_protocol,
    max(is_pool) AS is_pool,
    max(is_lending_user) AS is_lending_user,
    max(is_validator_depositor) AS is_validator_depositor,
    max(has_dune_label) AS has_dune_label,
    anyIf(dune_project, dune_project != '') AS dune_project
FROM all_rows
WHERE address IS NOT NULL AND address != ''
GROUP BY address
