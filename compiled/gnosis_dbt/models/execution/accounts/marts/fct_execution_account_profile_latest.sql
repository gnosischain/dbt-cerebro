

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
  FROM `dbt`.`fct_execution_address_resolver`
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
  FROM `dbt`.`fct_execution_account_token_balances_latest`
  GROUP BY address
),

movement_summary AS (
  SELECT
    address,
    first_activity_date,
    last_activity_date,
    counterparty_count,
    token_transfer_count
  FROM `dbt`.`fct_execution_account_transaction_summary_latest`
),

linked_summary AS (
  SELECT
    root_address AS address,
    count() AS linked_entity_count,
    countIf(relation = 'safe_owner_of') AS linked_safe_count,
    countIf(relation = 'safe_owned_by') AS linked_safe_owner_count,
    sumIf(value_count, relation = 'validator_withdrawal_credential') AS linked_validator_count
  FROM `dbt`.`fct_execution_account_linked_entities_latest`
  GROUP BY address
),

ga_users AS (
  SELECT
    lower(address) AS address,
    first_seen_at AS gnosis_app_first_seen_at,
    last_seen_at AS gnosis_app_last_seen_at,
    heuristic_hits AS gnosis_app_heuristic_hits,
    n_distinct_heuristics AS gnosis_app_heuristic_count
  FROM `dbt`.`int_execution_gnosis_app_users_current`
),

ga_gpay AS (
  SELECT
    lower(first_ga_owner_address) AS address,
    any(lower(pay_wallet)) AS controlled_gpay_wallet,
    countIf(is_currently_ga_owned = 1) AS controlled_gpay_wallet_count
  FROM `dbt`.`int_execution_gnosis_app_gpay_wallets`
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
  FROM `dbt`.`fct_execution_gpay_user_lifetime_metrics`
),

yields AS (
  SELECT
    lower(wallet_address) AS address,
    total_lp_fees_usd,
    total_lending_balance_usd,
    active_lp_positions,
    active_lending_positions,
    first_yield_date
  FROM `dbt`.`fct_execution_yields_user_lifetime_metrics`
)

SELECT
  coalesce(r.address, b.address, m.address, ga.address, gg.address, gp.address, y.address) AS address,
  coalesce(nullIf(r.display_name, ''), r.address, b.address, m.address, ga.address, gg.address, gp.address, y.address) AS display_name,
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
    coalesce(toDate(ga.gnosis_app_first_seen_at), toDate('2100-01-01'))
  ]), toDate('2100-01-01')) AS first_seen_date,
  nullIf(arrayMax([
    coalesce(m.last_activity_date, toDate('1970-01-01')),
    coalesce(gp.gpay_last_activity_date, toDate('1970-01-01')),
    coalesce(toDate(ga.gnosis_app_last_seen_at), toDate('1970-01-01'))
  ]), toDate('1970-01-01')) AS last_active_date,
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
FROM resolver_merged r
FULL OUTER JOIN balance_summary b ON b.address = r.address
FULL OUTER JOIN movement_summary m ON m.address = coalesce(r.address, b.address)
FULL OUTER JOIN ga_users ga ON ga.address = coalesce(r.address, b.address, m.address)
FULL OUTER JOIN ga_gpay gg ON gg.address = coalesce(r.address, b.address, m.address, ga.address)
FULL OUTER JOIN gpay gp ON gp.address = coalesce(r.address, b.address, m.address, ga.address, gg.address)
FULL OUTER JOIN yields y ON y.address = coalesce(r.address, b.address, m.address, ga.address, gg.address, gp.address)
LEFT JOIN linked_summary ls ON ls.address = coalesce(r.address, b.address, m.address, ga.address, gg.address, gp.address, y.address)