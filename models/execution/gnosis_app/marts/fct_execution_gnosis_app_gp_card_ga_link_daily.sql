{{
  config(
    materialized='table',
    engine='MergeTree()',
    order_by='(date, link_source)',
    tags=['production','execution','gnosis_app','gpay','mart']
  )
}}

-- Aggregate, privacy-safe daily series of Gnosis-App-LINKED Gnosis Pay cards, split by link_source.
-- Backing fact for api_execution_gnosis_app_gp_card_ga_link_daily. Counts only — no card/account
-- address or pseudonym crosses into this layer. Each card carries exactly ONE (highest-precedence)
-- link_source in the upstream model, so summing across sources = total distinct GA-linked cards
-- (no double count). This is the module-agnostic recovery series that resumes past the June-2026
-- plateau of the DelayModule-only api_execution_gnosis_app_gpay_wallets_daily; it is a LINK series,
-- NOT the onboarded_via_ga-vs-imported class (which is unrecoverable post-migration).

WITH base AS (
    -- assumeNotNull keeps `date` and `link_source` NON-nullable so the MergeTree order_by
    -- (date, link_source) is valid; both are guaranteed non-null by the WHERE below.
    SELECT
        toDate(assumeNotNull(first_linked_at)) AS date,
        assumeNotNull(link_source)             AS link_source,
        card                                   AS card
    FROM {{ ref('int_execution_gnosis_app_gp_card_ga_link') }}
    WHERE first_linked_at IS NOT NULL
      AND link_source IS NOT NULL
),

daily AS (
    SELECT
        date,
        link_source,
        count(DISTINCT card) AS n_cards_new
    FROM base
    GROUP BY date, link_source
),

-- Dense calendar spine × link_source so the chart has continuous dates.
calendar AS (
    SELECT addDays(min_date, number) AS date
    FROM (
        SELECT min(date) AS min_date, today() AS max_date
        FROM daily
    )
    ARRAY JOIN range(0, toUInt64(dateDiff('day', min_date, max_date) + 1)) AS number
),

sources AS (
    SELECT DISTINCT link_source FROM base
),

spine AS (
    SELECT c.date, s.link_source
    FROM calendar c CROSS JOIN sources s
)

SELECT
    s.date                                                       AS date,
    s.link_source                                                AS link_source,
    coalesce(d.n_cards_new, 0)                                   AS n_cards_new,
    sum(coalesce(d.n_cards_new, 0))
        OVER (PARTITION BY s.link_source
              ORDER BY s.date
              ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)  AS n_cards_cumulative
FROM spine s
LEFT JOIN daily d
    ON d.date = s.date
   AND d.link_source = s.link_source
ORDER BY s.date, s.link_source
