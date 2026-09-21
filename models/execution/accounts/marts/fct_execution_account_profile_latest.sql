{{
  config(
    materialized='table',
    engine='ReplacingMergeTree()',
    order_by='(address)',
    unique_key='address',
    settings={ 'allow_nullable_key': 1 },
    pre_hook=["SET join_use_nulls = 1"],
    post_hook=["SET join_use_nulls = 0"],
    tags=['production', 'execution', 'accounts', 'portfolio', 'profile', 'granularity:latest']
  )
}}

-- Address spine, then plain left joins.
--
-- This model used to end in six chained FULL OUTER JOINs whose keys were a growing
-- coalesce(): `ON ga.address = coalesce(r.address, b.address, m.address)` and so on. A
-- computed join key cannot use the sort order, so every step materialised both sides in
-- full, and ClickHouse buffers both sides of a FULL JOIN anyway. On a few hundred MB of
-- input that cost 8.34 GiB and 338 s (measured 2026-09-21), which is why it carried
-- single-threading, grace_hash and a 10 GB allowance -- on a server whose total ceiling is
-- 10.8 GiB. Holding ~8 GiB for five minutes nightly is what starved everything running
-- beside it, producing the recurring `(total) memory limit exceeded` failures elsewhere.
--
-- A FULL OUTER chain emits exactly the rows whose key appears in at least one input, which
-- is the union of the keys. Building that union once as a spine and left-joining each source
-- on a plain sorted column is equivalent by construction. Verified on production data:
-- identical address sets (1,725,101 rows each, none exclusive to either side) and zero rows
-- differing on any column. Cost after: 2.75 GiB and 6.3 s with NO tuning at all.
-- The resolver fct now stores one row per (address × source). We collapse
-- it into a per-address merged CTE here so the rest of this model can join
-- on the resolver as if it were one-row-per-address (its old shape).
--
-- Two-stage CTE: first aggregate with `_m` aliases (avoids `max(col) AS col`
-- alias collisions and nested-aggregate errors); then compose display_name
-- using the already-aggregated columns.
WITH resolver_agg AS (
  SELECT
    address,
    max(is_safe) > 0                          AS is_safe_m,
    max(is_safe_owner) > 0                    AS is_safe_owner_m,
    max(is_circles_avatar) > 0                AS is_circles_avatar_m,
    max(is_gpay_wallet) > 0                   AS is_gpay_wallet_m,
    max(is_validator_withdrawal_address) > 0  AS is_validator_wd_m,
    max(connected_safe_count)                 AS connected_safe_m,
    max(connected_validator_count)            AS connected_validator_m,
    max(circles_name)                         AS circles_name_m,
    max(circles_avatar_type)                  AS circles_avatar_type_m
  FROM {{ ref('fct_execution_address_resolver') }}
  GROUP BY address
),

resolver_merged AS (
  SELECT
    address,
    is_safe_m              AS is_safe,
    is_safe_owner_m        AS is_safe_owner,
    is_circles_avatar_m    AS is_circles_avatar,
    is_gpay_wallet_m       AS is_gpay_wallet,
    is_validator_wd_m      AS is_validator_withdrawal_address,
    connected_safe_m       AS connected_safe_count,
    connected_validator_m  AS connected_validator_count,
    circles_name_m         AS circles_name,
    circles_avatar_type_m  AS circles_avatar_type,
    COALESCE(
      circles_name_m,
      multiIf(
        connected_validator_m > 0,
          concat('Validator operator · ', toString(connected_validator_m), ' validators'),
        connected_safe_m > 0,
          concat('Safe owner · ', toString(connected_safe_m), ' safes'),
        is_safe_m,           'Safe contract',
        is_gpay_wallet_m,    'Gnosis Pay wallet',
        is_circles_avatar_m, 'Circles avatar',
        ''
      )
    ) AS display_name
  FROM resolver_agg
),

balance_summary AS (
  SELECT
    address,
    sum(balance_usd) AS total_balance_usd,
    count() AS tokens_held,
    maxIf(balance, upper(symbol) IN ('XDAI', 'WXDAI')) AS native_or_wrapped_xdai_balance,
    max(date) AS balance_date
  FROM {{ ref('fct_execution_account_token_balances_latest') }}
  GROUP BY address
),

movement_summary AS (
  SELECT
    address,
    first_activity_date,
    last_activity_date,
    counterparty_count,
    token_transfer_count
  FROM {{ ref('fct_execution_account_transaction_summary_latest') }}
),

linked_summary AS (
  SELECT
    root_address AS address,
    count() AS linked_entity_count,
    countIf(relation = 'safe_owner_of') AS linked_safe_count,
    countIf(relation = 'safe_owned_by') AS linked_safe_owner_count,
    sumIf(value_count, relation = 'validator_withdrawal_credential') AS linked_validator_count
  FROM {{ ref('fct_execution_account_linked_entities_latest') }}
  GROUP BY address
),

ga_users AS (
  SELECT
    lower(address) AS address,
    first_seen_at AS gnosis_app_first_seen_at,
    last_seen_at AS gnosis_app_last_seen_at,
    heuristic_hits AS gnosis_app_heuristic_hits,
    n_distinct_heuristics AS gnosis_app_heuristic_count
  FROM {{ ref('int_execution_gnosis_app_users_current') }}
),

ga_gpay AS (
  SELECT
    lower(first_ga_owner_address) AS address,
    any(lower(pay_wallet)) AS controlled_gpay_wallet,
    countIf(is_currently_ga_owned = 1) AS controlled_gpay_wallet_count
  FROM {{ ref('int_execution_gnosis_app_gpay_wallets') }}
  WHERE first_ga_owner_address IS NOT NULL
  GROUP BY address
),

gpay AS (
  SELECT
    lower(wallet_address) AS address,
    first_activity_date AS gpay_first_activity_date,
    last_activity_date AS gpay_last_activity_date,
    total_payment_volume_usd AS gpay_total_payment_volume_usd,
    total_payment_count AS gpay_total_payment_count
  FROM {{ ref('fct_execution_gpay_user_lifetime_metrics') }}
),

yields AS (
  SELECT
    lower(wallet_address) AS address,
    total_lp_fees_usd,
    total_lending_balance_usd,
    active_lp_positions,
    active_lending_positions,
    first_yield_date
  FROM {{ ref('fct_execution_yields_user_lifetime_metrics') }}
),

safe_creation AS (
  SELECT
    lower(safe_address) AS address,
    block_date AS safe_creation_date
  FROM {{ ref('int_execution_safes') }}
),

address_spine AS (
  SELECT DISTINCT address FROM (
    SELECT address FROM resolver_merged
    UNION ALL SELECT address FROM balance_summary
    UNION ALL SELECT address FROM movement_summary
    UNION ALL SELECT address FROM ga_users
    UNION ALL SELECT address FROM ga_gpay
    UNION ALL SELECT address FROM gpay
    UNION ALL SELECT address FROM yields
    UNION ALL SELECT address FROM safe_creation
  )
)

SELECT
  s.address AS address,
  coalesce(nullIf(r.display_name, ''), s.address) AS display_name,
  r.is_safe,
  r.is_safe_owner,
  r.is_circles_avatar,
  r.is_gpay_wallet OR gp.address IS NOT NULL AS is_gpay_wallet,
  r.is_validator_withdrawal_address,
  ga.address IS NOT NULL AS is_gnosis_app_user,
  y.active_lp_positions > 0 AS is_lp_provider,
  y.active_lending_positions > 0 AS is_lending_user,
  (y.active_lp_positions > 0 OR y.active_lending_positions > 0) AS has_yield_activity,
  r.connected_safe_count,
  r.connected_validator_count,
  r.circles_name,
  r.circles_avatar_type,
  b.total_balance_usd,
  b.tokens_held,
  b.native_or_wrapped_xdai_balance,
  b.balance_date,
  nullIf(arrayMin([
    coalesce(m.first_activity_date, toDate('2100-01-01')),
    coalesce(gp.gpay_first_activity_date, toDate('2100-01-01')),
    coalesce(y.first_yield_date, toDate('2100-01-01')),
    coalesce(toDate(ga.gnosis_app_first_seen_at), toDate('2100-01-01')),
    coalesce(sc.safe_creation_date, toDate('2100-01-01'))
  ]), toDate('2100-01-01')) AS first_seen_date,
  nullIf(arrayMax([
    coalesce(m.last_activity_date, toDate('1970-01-01')),
    coalesce(gp.gpay_last_activity_date, toDate('1970-01-01')),
    coalesce(toDate(ga.gnosis_app_last_seen_at), toDate('1970-01-01'))
  ]), toDate('1970-01-01')) AS last_active_date,
  sc.safe_creation_date,
  multiIf(r.is_safe, 'safe', 'eoa_or_contract') AS address_type,
  coalesce(
    sc.safe_creation_date,
    nullIf(arrayMin([
      coalesce(m.first_activity_date, toDate('2100-01-01')),
      coalesce(gp.gpay_first_activity_date, toDate('2100-01-01')),
      coalesce(y.first_yield_date, toDate('2100-01-01')),
      coalesce(toDate(ga.gnosis_app_first_seen_at), toDate('2100-01-01'))
    ]), toDate('2100-01-01'))
  ) AS wallet_age_date,
  m.counterparty_count,
  m.token_transfer_count,
  coalesce(ls.linked_entity_count, 0) AS linked_entity_count,
  coalesce(ls.linked_safe_count, r.connected_safe_count, 0) AS linked_safe_count,
  coalesce(ls.linked_safe_owner_count, 0) AS linked_safe_owner_count,
  coalesce(ls.linked_validator_count, r.connected_validator_count, 0) AS linked_validator_count,
  gg.controlled_gpay_wallet,
  coalesce(gg.controlled_gpay_wallet_count, 0) AS controlled_gpay_wallet_count,
  ga.gnosis_app_first_seen_at,
  ga.gnosis_app_last_seen_at,
  ga.gnosis_app_heuristic_hits,
  ga.gnosis_app_heuristic_count,
  gp.gpay_total_payment_volume_usd,
  gp.gpay_total_payment_count,
  y.total_lp_fees_usd,
  y.total_lending_balance_usd,
  y.active_lp_positions,
  y.active_lending_positions
FROM address_spine s
LEFT JOIN resolver_merged  r  ON r.address  = s.address
LEFT JOIN balance_summary  b  ON b.address  = s.address
LEFT JOIN movement_summary m  ON m.address  = s.address
LEFT JOIN ga_users         ga ON ga.address = s.address
LEFT JOIN ga_gpay          gg ON gg.address = s.address
LEFT JOIN gpay             gp ON gp.address = s.address
LEFT JOIN yields           y  ON y.address  = s.address
LEFT JOIN safe_creation    sc ON sc.address = s.address
LEFT JOIN linked_summary   ls ON ls.address = s.address
