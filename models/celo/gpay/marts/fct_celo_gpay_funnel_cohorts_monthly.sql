{{
  config(
    materialized='table',
    engine='ReplacingMergeTree()',
    order_by='cohort_month',
    tags=['production','celo','gpay']
  )
}}

-- Cards grouped by the month they were ISSUED, with how far each cohort got through
-- issued > funded > activated. Rollup of fct_celo_gpay_card_funnel.
--
-- COHORTED AND AGE-NORMALISED, both deliberately. A single all-time "62% of cards get
-- funded" is not a conversion rate on this product: issuance is still ramping, so the
-- denominator is dominated by recently issued cards that have had days rather than
-- months to convert, and the headline moves when issuance accelerates even if user
-- behaviour is identical. Cohorting removes the mix shift; the _7d/_30d windows remove
-- the age difference within a cohort.
--
-- READ THE WINDOWED RATES, NOT THE _ever COLUMNS, when comparing cohorts. funded_ever
-- and activated_ever are honest counts as of the horizon but they are NOT comparable
-- across rows — March cards have had four months to convert and July cards have had
-- days, so _ever will always slope down toward the present and that slope is an
-- artefact. funded_rate_7d compares like with like because every card counted in it has
-- had exactly the same seven days.
--
-- ELIGIBILITY IS THE DENOMINATOR, NOT THE COHORT SIZE. A card only enters the 7-day rate
-- once it has 7 days of observation; eligible_7d is that count and can be smaller than
-- cards_issued for the current cohort. cohort_complete_7d says whether every card in the
-- row has cleared the window — a FALSE there means the rate is computed on a subset and
-- will still move. The current month is always incomplete and its 30-day figures are
-- usually null-rated (no eligible cards at all) for the first month of its life; that is
-- correct, not missing data.
--
-- THE LAG MEDIANS ARE COMPLETER-BIASED and cannot be fixed by an eligibility gate. They
-- are taken over cards that DID convert, so a young cohort's median only knows about its
-- fast converters — the slow ones have not converted yet and are invisible rather than
-- large. Expect medians to drift upward as a cohort matures. Compare them only between
-- rows where cohort_complete_30d is TRUE, and treat the newest row's median as a floor.
--
-- Windows are 7 and 30 days from ISSUANCE for both milestones, one clock rather than
-- measuring activation from funding, so the two rates share a denominator and a card
-- cannot be activated-but-not-funded in the same window.

WITH counted AS (
    SELECT
        toStartOfMonth(issued_at)                                     AS cohort_month,
        count()                                                       AS cards_issued,

        -- Cards old enough to be judged at each window.
        countIf(observation_days >= 7)                                AS eligible_7d,
        countIf(observation_days >= 30)                               AS eligible_30d,

        countIf(observation_days >= 7  AND days_issue_to_fund  <= 7)  AS funded_7d,
        countIf(observation_days >= 30 AND days_issue_to_fund  <= 30) AS funded_30d,
        countIf(is_funded)                                            AS funded_ever,

        countIf(observation_days >= 7  AND days_issue_to_spend <= 7)  AS activated_7d,
        countIf(observation_days >= 30 AND days_issue_to_spend <= 30) AS activated_30d,
        countIf(is_activated)                                         AS activated_ever,

        -- FALSE means at least one card in the row has not cleared the window, so the
        -- rate is provisional.
        min(observation_days) >= 7                                    AS cohort_complete_7d,
        min(observation_days) >= 30                                   AS cohort_complete_30d,

        -- Lags over converters only. quantileExact rather than quantile: cohorts are
        -- small enough that the sampling approximation is visible noise.
        toInt32(quantileExactIf(0.5)(days_issue_to_fund,  days_issue_to_fund  IS NOT NULL)) AS median_days_issue_to_fund,
        toInt32(quantileExactIf(0.9)(days_issue_to_fund,  days_issue_to_fund  IS NOT NULL)) AS p90_days_issue_to_fund,
        toInt32(quantileExactIf(0.5)(days_fund_to_spend,  days_fund_to_spend  IS NOT NULL)) AS median_days_fund_to_spend,
        toInt32(quantileExactIf(0.9)(days_fund_to_spend,  days_fund_to_spend  IS NOT NULL)) AS p90_days_fund_to_spend,

        max(observed_through)                                         AS observed_through
    FROM {{ ref('fct_celo_gpay_card_funnel') }}
    GROUP BY cohort_month
)

SELECT
    cohort_month,
    cards_issued,
    eligible_7d,
    eligible_30d,
    funded_7d,
    funded_30d,
    funded_ever,
    activated_7d,
    activated_30d,
    activated_ever,
    -- nullIf on the denominator: a cohort with no eligible cards yet has an UNDEFINED
    -- rate, and returning 0 there would draw a cliff on every dashboard at the current
    -- month that readers consistently mistake for a collapse in conversion.
    round(funded_7d     / nullIf(eligible_7d,  0), 4)  AS funded_rate_7d,
    round(funded_30d    / nullIf(eligible_30d, 0), 4)  AS funded_rate_30d,
    round(activated_7d  / nullIf(eligible_7d,  0), 4)  AS activated_rate_7d,
    round(activated_30d / nullIf(eligible_30d, 0), 4)  AS activated_rate_30d,
    -- Funded cards that went on to spend, within the same window. The second leg of the
    -- funnel on its own, which is where the drop-off actually is.
    round(activated_7d  / nullIf(funded_7d,  0), 4)    AS spend_rate_of_funded_7d,
    round(activated_30d / nullIf(funded_30d, 0), 4)    AS spend_rate_of_funded_30d,
    cohort_complete_7d,
    cohort_complete_30d,
    median_days_issue_to_fund,
    p90_days_issue_to_fund,
    median_days_fund_to_spend,
    p90_days_fund_to_spend,
    observed_through
FROM counted
ORDER BY cohort_month
