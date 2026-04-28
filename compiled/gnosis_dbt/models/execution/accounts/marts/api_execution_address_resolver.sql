

-- Per-address merge on top of fct_execution_address_resolver. The fct table
-- carries one row per (address × source) signal — we collapse them into a
-- single row per address here, at query time, with an indexed point-lookup
-- aggregation. Building the merge into the fct itself OOMs the 10.8 GiB
-- cluster cap on a 5–10M-address GROUP BY; the dashboard always filters by
-- a single address (`require_any_of: [address]`) so the merge cost is
-- O(≤5 rows) per request.
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
      is_safe_m,                              'Safe contract',
      is_gpay_wallet_m,                       'Gnosis Pay wallet',
      is_circles_avatar_m,                    'Circles avatar',
      ''
    )
  ) AS display_name
FROM (
  -- Aggregate per address with distinct intermediate aliases — ClickHouse
  -- collides `max(col) AS col` with the source column name and rewrites it
  -- into a nested aggregate at compile time.
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
)