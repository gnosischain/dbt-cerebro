

-- GA-LINKED Gnosis Pay funding & spend volume, daily, split by link_source. The module-agnostic,
-- correctly-timed successor to fct_execution_gnosis_app_gpay_volume_daily (which is DelayModule-only
-- AND joins activity on the OLD safe address, so migrated cards' activity — now on the NEW canonical
-- safe — falls out and the series DECLINES post-June).
--
-- Fixes both:
--   (1) card set = int_execution_gnosis_app_gp_card_ga_link (all architectures, canonical cards);
--   (2) int_execution_gpay_activity.wallet_address is CANONICALIZED (old -> new) before the join, so
--       a migrated card's new-safe activity attributes to the same canonical card.
--
-- Attribution timestamp per card:
--   ga_control_start_at = coalesce(first_ga_owner_at, first_card_activity_at)
--     first_ga_owner_at      — frozen on-chain "a registered GA account became owner" time
--                              (int_execution_gnosis_app_gpay_wallets), canonicalized. Authoritative;
--                              keeps an IMPORTED card's pre-ownership spend OUT.
--     first_card_activity_at — the card's first activity (min block_timestamp), the fallback for
--                              net-new / Mixpanel-only / top-up / cashback cards ("from first payment").
--   coalesce PRIORITY (not least): a Delay card keeps its real ownership time even if it had earlier
--   non-GA activity. Aggregate counts/sums only — no address/pseudonym leaves this model.

WITH

canon AS (
    SELECT address, canonical_address
    FROM `dbt`.`int_execution_gpay_safe_canonical`
),

-- Frozen DelayModule owner-time, collapsed to the canonical card key.
owner_time AS (
    SELECT
        if(c.canonical_address != '' AND c.canonical_address IS NOT NULL, c.canonical_address, lower(w.pay_wallet)) AS card,
        min(w.first_ga_owner_at) AS first_ga_owner_at
    FROM `dbt`.`int_execution_gnosis_app_gpay_wallets` w
    LEFT JOIN canon c ON c.address = lower(w.pay_wallet)
    WHERE w.first_ga_owner_at IS NOT NULL
    GROUP BY card
),

-- GA-linked cards (already canonical) + their optional owner-time.
cards AS (
    SELECT
        l.card                     AS card,
        l.link_source              AS link_source,
        ot.first_ga_owner_at       AS first_ga_owner_at
    FROM `dbt`.`int_execution_gnosis_app_gp_card_ga_link` l
    LEFT JOIN owner_time ot ON ot.card = l.card
),

-- All GP activity, canonicalized (old -> new Safe) to the card key.
activity_canon AS (
    SELECT
        if(c.canonical_address != '' AND c.canonical_address IS NOT NULL, c.canonical_address, lower(a.wallet_address)) AS card,
        a.block_timestamp                        AS block_timestamp,
        a.action                                 AS action,
        toFloat64OrNull(toString(a.amount_usd))  AS amount_usd
    FROM `dbt`.`int_execution_gpay_activity` a
    LEFT JOIN canon c ON c.address = lower(a.wallet_address)
    WHERE a.action IN ('Fiat Top Up','Crypto Deposit','Payment')
      AND a.block_timestamp < today()
),

-- Restrict to the GA-linked card set (semi-join on the canonical key).
linked_activity AS (
    SELECT
        ac.card               AS card,
        cd.link_source        AS link_source,
        cd.first_ga_owner_at  AS first_ga_owner_at,
        ac.block_timestamp    AS block_timestamp,
        ac.action             AS action,
        ac.amount_usd         AS amount_usd
    FROM activity_canon ac
    INNER JOIN cards cd ON cd.card = ac.card
),

-- Per-card control-start = owner-time if present, else first activity.
-- first_ga_owner_at is constant per card; ClickHouse LEFT JOIN (join_use_nulls=0) fills the
-- non-Delay cards with the epoch default (1970), so guard on a sanity floor: a real owner-time
-- (> 2015) wins, otherwise fall back to the card's first activity ("from first payment").
control AS (
    SELECT
        card,
        if(max(first_ga_owner_at) > toDateTime('2015-01-01'),
           max(first_ga_owner_at),
           min(block_timestamp)) AS ga_control_start_at
    FROM linked_activity
    GROUP BY card
),

agg AS (
    SELECT
        toDate(la.block_timestamp)                                                    AS date,
        la.link_source                                                                AS link_source,
        sumIf(la.amount_usd, la.action IN ('Fiat Top Up','Crypto Deposit'))           AS funded_volume_usd,
        sumIf(la.amount_usd, la.action = 'Payment')                                   AS spend_usd,
        countIf(la.action = 'Payment')                                                AS spend_count,
        uniqExactIf(la.card, la.action = 'Payment')                                   AS spending_cards
    FROM linked_activity la
    INNER JOIN control ct ON ct.card = la.card
    WHERE la.block_timestamp >= ct.ga_control_start_at
    GROUP BY date, link_source
),

-- Dense calendar spine × link_source so the cumulative series stays continuous.
calendar AS (
    SELECT addDays(min_date, number) AS date
    FROM (
        SELECT min(date) AS min_date, today() AS max_date
        FROM agg
    )
    ARRAY JOIN range(0, toUInt64(dateDiff('day', min_date, max_date) + 1)) AS number
),

sources AS (
    SELECT DISTINCT link_source FROM agg
),

spine AS (
    SELECT c.date, s.link_source
    FROM calendar c CROSS JOIN sources s
)

SELECT
    s.date                                                            AS date,
    s.link_source                                                     AS link_source,
    round(coalesce(a.funded_volume_usd, 0), 2)                        AS funded_volume_usd,
    round(coalesce(a.spend_usd, 0), 2)                                AS spend_usd,
    coalesce(a.spend_count, 0)                                        AS spend_count,
    coalesce(a.spending_cards, 0)                                     AS spending_cards,
    round(sum(coalesce(a.funded_volume_usd, 0))
        OVER (PARTITION BY s.link_source
              ORDER BY s.date
              ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), 2)   AS funded_volume_cumulative_usd,
    round(sum(coalesce(a.spend_usd, 0))
        OVER (PARTITION BY s.link_source
              ORDER BY s.date
              ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), 2)   AS spend_cumulative_usd
FROM spine s
LEFT JOIN agg a
    ON a.date = s.date
   AND a.link_source = s.link_source
ORDER BY s.date, s.link_source