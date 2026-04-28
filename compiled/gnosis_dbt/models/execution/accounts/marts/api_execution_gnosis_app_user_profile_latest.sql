

SELECT
  address,
  first_seen_at,
  last_seen_at,
  heuristic_hits,
  heuristic_kinds,
  n_distinct_heuristics,
  controlled_gpay_wallet,
  is_currently_ga_owned,
  n_ga_owners_current,
  n_total_owners_current,
  onboarding_class
FROM `dbt`.`fct_execution_gnosis_app_user_profile_latest`