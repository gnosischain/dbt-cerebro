-- Min-floor data-loss guard for the gnosis_app_gt suite (NEVER equality — the
-- envio_ga source drifts UP continuously). Returns offending rows only, so the
-- test passes iff every floor holds. Floors are ~15-20% below the values
-- validated 2026-07-01 (registry 301,596 / avatar 173,908 / native-filled swaps
-- 57,434 / guardian membership 139,919 / referral edges ~11,548).
WITH m AS (
    SELECT
        (SELECT count() FROM {{ ref('stg_envio_ga__users') }})                         AS registry,
        (SELECT count() FROM {{ ref('stg_envio_ga__avatars') }})                       AS avatars,
        (SELECT countIf(app_scope IN ('gnosis_app', 'metri') AND status = 'Filled') FROM {{ ref('stg_envio_ga__swaps') }}) AS native_filled_swaps,
        (SELECT count() FROM {{ ref('int_execution_gnosis_app_gt_pay_wallets') }})      AS pay_wallets,
        (SELECT count() FROM {{ ref('stg_envio_ga__earned_from_invite') }})            AS referral_edges
)
SELECT *
FROM m
WHERE registry            < 250000
   OR avatars             < 150000
   OR native_filled_swaps < 45000
   OR pay_wallets         < 110000
   OR referral_edges      < 9000
