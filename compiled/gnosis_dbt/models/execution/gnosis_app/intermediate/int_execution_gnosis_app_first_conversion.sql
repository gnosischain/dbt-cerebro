

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
        -- minIf returns the Date default (1970-01-01), NOT NULL, for a user
        -- with no row of this kind; nullIf maps that sentinel back to NULL so
        -- non-converters are genuinely NULL (1970-01-01 cannot occur in real
        -- 2025+ data). The join_use_nulls hook handles the same default for
        -- users who miss the LEFT JOIN entirely.
        nullIf(minIf(date, activity_kind = 'topup'),             toDate('1970-01-01')) AS first_topup_at,
        nullIf(minIf(date, activity_kind = 'swap_filled'),       toDate('1970-01-01')) AS first_swap_filled_at,
        nullIf(minIf(date, activity_kind = 'marketplace_buy'),   toDate('1970-01-01')) AS first_marketplace_buy_at,
        nullIf(minIf(date, activity_kind = 'token_offer_claim'), toDate('1970-01-01')) AS first_token_offer_claim_at
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