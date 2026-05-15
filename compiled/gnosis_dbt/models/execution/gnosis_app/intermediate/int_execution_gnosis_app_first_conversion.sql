

-- One row per Gnosis App user with the date of their first event in each
-- conversion category. Used by the time-to-first-conversion cohort.
--
-- Conversion kinds tracked (mirrors the activity_kind enum minus `onboard`):
--   first_topup_at
--   first_swap_filled_at
--   first_marketplace_buy_at
--   first_token_offer_claim_at
--
-- `first_seen_at` is the onboard date (heuristic-derived; carried from
-- int_execution_gnosis_app_users_current via the user_activity_daily view).
-- Cohort grouping in the api_ view buckets by toStartOfMonth(first_seen_at).
--
-- Full rebuild; size is bounded by ~total GA users (currently ~tens of k).

WITH onboard AS (
    SELECT
        address,
        min(date) AS first_seen_at
    FROM `dbt`.`int_execution_gnosis_app_user_activity_daily`
    WHERE activity_kind = 'onboard'
    GROUP BY address
),

firsts AS (
    SELECT
        address,
        minIf(date, activity_kind = 'topup')             AS first_topup_at,
        minIf(date, activity_kind = 'swap_filled')       AS first_swap_filled_at,
        minIf(date, activity_kind = 'marketplace_buy')   AS first_marketplace_buy_at,
        minIf(date, activity_kind = 'token_offer_claim') AS first_token_offer_claim_at
    FROM `dbt`.`int_execution_gnosis_app_user_activity_daily`
    WHERE activity_kind IN ('topup', 'swap_filled', 'marketplace_buy', 'token_offer_claim')
    GROUP BY address
)

SELECT
    o.address                                                     AS address,
    o.first_seen_at                                               AS first_seen_at,
    f.first_topup_at                                              AS first_topup_at,
    f.first_swap_filled_at                                        AS first_swap_filled_at,
    f.first_marketplace_buy_at                                    AS first_marketplace_buy_at,
    f.first_token_offer_claim_at                                  AS first_token_offer_claim_at
FROM onboard o
LEFT JOIN firsts f ON f.address = o.address