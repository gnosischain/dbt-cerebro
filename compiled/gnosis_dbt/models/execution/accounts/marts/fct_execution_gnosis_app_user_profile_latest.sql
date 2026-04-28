

SELECT
  lower(u.address) AS address,
  u.first_seen_at,
  u.last_seen_at,
  u.heuristic_hits,
  u.heuristic_kinds,
  u.n_distinct_heuristics,
  g.pay_wallet AS controlled_gpay_wallet,
  g.is_currently_ga_owned,
  g.n_ga_owners_current,
  g.n_total_owners_current,
  g.onboarding_class
FROM `dbt`.`int_execution_gnosis_app_users_current` u
LEFT JOIN `dbt`.`int_execution_gnosis_app_gpay_wallets` g
  ON lower(g.first_ga_owner_address) = lower(u.address)