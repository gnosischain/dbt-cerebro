

-- One row per CANONICAL Gnosis Pay card, flagged as controlled/funded by a Gnosis App account.
--
-- Sources (precedence, highest first):
--   1 mixpanel_pay  — Mixpanel profile `pay` property (int_mixpanel_ga_gpay_pay_bridge), the app's own
--                     record of which GA account owns which GP Safe. Pseudonym-keyed; resolved to a
--                     canonical card via the gp_pseudonym_map CTE below, which pseudonymizes EVERY known
--                     GP Safe address (current + pre-migration old) so old-address `pay` values still
--                     resolve. Module-agnostic -> survives the RolesModule migration.
--   2 delay_module  — legacy DelayModule first-GA-owner (pre-June; carried onto the canonical Safe below).
--   3 topup_funder  — a registered GA account funded the card via a Cometh-relayed top-up.
--   4 cashback      — cashback-NFT owner linked to the card.
--   (2-4 come pre-unioned from int_execution_gnosis_app_gt_card_owner.)
--
-- All cards are collapsed to their June-2026 canonical (new) Safe via int_execution_gpay_safe_canonical
-- BEFORE de-duplication, so a migrated card reachable via both a legacy Delay edge and a fresh
-- Mixpanel/Cometh edge is counted once (dup-safe). first_linked_at is the EARLIEST evidence date across
-- {legacy first_ga_owner_at, cashback minted_at, Safe safe_setup}; using the pre-June legacy date for
-- migrated cards avoids a false June "migration spike" on the cumulative chart.

WITH

canon AS (
    SELECT address, canonical_address
    FROM `dbt`.`int_execution_gpay_safe_canonical`
),

-- Comprehensive pseudonym map: the pseudonym of EVERY known GP Safe address (every current Safe
-- in the registry, PLUS the pre-migration OLD address of every migrated card) -> its canonical
-- card. The Mixpanel `pay` profile can still record the OLD Safe address, whose pseudonym is NOT
-- present in the shared safe_self identity (keyed only on the canonical Safe) — which previously
-- dropped ~760 Mixpanel-linked cards. Mapping BOTH address forms here makes every profile `pay`
-- value that corresponds to a known GP Safe resolve. (pseudonymize_address is injective, so each
-- pay pseudonym matches at most one map row -> no fan-out.)
gp_pseudonym_map AS (
    SELECT
        
    sipHash64(concat(unhex('00'), lower(lower(w.address))))
                                                        AS pseudonym,
        if(c.canonical_address != '' AND c.canonical_address IS NOT NULL, c.canonical_address, lower(w.address)) AS card
    FROM `dbt`.`int_execution_gpay_wallets` w
    LEFT JOIN canon c ON c.address = lower(w.address)

    UNION DISTINCT

    SELECT
        
    sipHash64(concat(unhex('00'), lower(c.address)))
  AS pseudonym,
        c.canonical_address                      AS card
    FROM `dbt`.`int_execution_gpay_safe_canonical` c
    WHERE c.canonical_address != '' AND c.canonical_address IS NOT NULL
),

-- Source 1: Mixpanel `pay`-profile bridge -> canonical card via the comprehensive pseudonym map.
mixpanel_pay AS (
    SELECT DISTINCT
        m.card          AS card,
        'mixpanel_pay'  AS link_source
    FROM `dbt`.`int_mixpanel_ga_gpay_pay_bridge` pb
    INNER JOIN gp_pseudonym_map m ON m.pseudonym = pb.pay_safe_pseudonym
    WHERE m.card IS NOT NULL AND m.card != ''
),

-- Source 2: FULL legacy DelayModule first-GA-owner set, taken DIRECTLY from the frozen model
-- (NOT via gt_card_owner, which gates the Delay signal to the envio-registered subset). This
-- guarantees the link is a strict SUPERSET of the frozen onboarding set — no known GA card is lost.
delay_full AS (
    SELECT DISTINCT
        lower(pay_wallet) AS card,
        'delay_module'    AS link_source
    FROM `dbt`.`int_execution_gnosis_app_gpay_wallets`
    WHERE first_ga_owner_address IS NOT NULL AND first_ga_owner_address != ''
),

-- Sources 3-4: cashback owner + top-up funder from the on-chain bridge (its delay_module row is
-- dropped here — superseded by the ungated delay_full above).
onchain AS (
    SELECT DISTINCT
        lower(card) AS card,
        source      AS link_source
    FROM `dbt`.`int_execution_gnosis_app_gt_card_owner`
    WHERE card IS NOT NULL AND card != ''
      AND source IN ('cashback', 'topup_funder')
),

all_links AS (
    SELECT card, link_source FROM mixpanel_pay
    UNION ALL
    SELECT card, link_source FROM delay_full
    UNION ALL
    SELECT card, link_source FROM onchain
),

-- Collapse each linked card onto its canonical (new) Safe.
canon_links AS (
    SELECT
        if(c.canonical_address != '' AND c.canonical_address IS NOT NULL, c.canonical_address, al.card) AS card,
        al.link_source                                                                                  AS link_source
    FROM all_links al
    LEFT JOIN canon c ON c.address = al.card
),

-- Highest-precedence source per canonical card.
ranked AS (
    SELECT
        card,
        link_source,
        multiIf(
            link_source = 'mixpanel_pay', 1,
            link_source = 'delay_module', 2,
            link_source = 'topup_funder', 3,
            link_source = 'cashback',     4,
            5
        ) AS rnk
    FROM canon_links
),

per_card AS (
    SELECT
        card,
        argMin(link_source, rnk) AS link_source
    FROM ranked
    GROUP BY card
),

-- Candidate first-seen dates (each canonicalized to the same key space).
delay_dates AS (
    SELECT
        if(c.canonical_address != '' AND c.canonical_address IS NOT NULL, c.canonical_address, lower(w.pay_wallet)) AS card,
        toDateTime(min(w.first_ga_owner_at)) AS d
    FROM `dbt`.`int_execution_gnosis_app_gpay_wallets` w
    LEFT JOIN canon c ON c.address = lower(w.pay_wallet)
    WHERE w.first_ga_owner_at IS NOT NULL
    GROUP BY card
),

cashback_dates AS (
    SELECT
        if(c.canonical_address != '' AND c.canonical_address IS NOT NULL, c.canonical_address, lower(cb.gnosis_pay_address)) AS card,
        toDateTime(min(cb.minted_at)) AS d
    FROM `dbt`.`stg_envio_ga__cashbacks` cb
    LEFT JOIN canon c ON c.address = lower(cb.gnosis_pay_address)
    WHERE cb.gnosis_pay_address IS NOT NULL AND cb.gnosis_pay_address != ''
    GROUP BY card
),

setup_dates AS (
    SELECT
        if(c.canonical_address != '' AND c.canonical_address IS NOT NULL, c.canonical_address, lower(oe.safe_address)) AS card,
        toDateTime(min(oe.block_timestamp)) AS d
    FROM `dbt`.`int_execution_safes_owner_events` oe
    LEFT JOIN canon c ON c.address = lower(oe.safe_address)
    WHERE oe.event_kind = 'safe_setup' AND oe.safe_address IS NOT NULL
    GROUP BY card
)

SELECT
    pc.card                                                          AS card,
    toUInt8(1)                                                       AS is_ga_linked,
    pc.link_source                                                   AS link_source,
    -- Earliest available evidence date across {legacy owner, cashback mint, Safe setup}.
    -- NB ClickHouse LEFT JOINs use join_use_nulls=0 by default, so an UNMATCHED date column is
    -- filled with the type default toDateTime(0)=1970-01-01 (NOT NULL). Guard each candidate by a
    -- sanity floor (< 2015 => "no date") so those epoch fills don't win the least(); a card with no
    -- real date at all resolves to the 2100 sentinel -> NULL and is dropped downstream.
    nullIf(
        least(
            if(dd.d  > toDateTime('2015-01-01'), dd.d,  toDateTime('2100-01-01 00:00:00')),
            if(cbd.d > toDateTime('2015-01-01'), cbd.d, toDateTime('2100-01-01 00:00:00')),
            if(sd.d  > toDateTime('2015-01-01'), sd.d,  toDateTime('2100-01-01 00:00:00'))
        ),
        toDateTime('2100-01-01 00:00:00')
    )                                                                AS first_linked_at
FROM per_card pc
LEFT JOIN delay_dates    dd  ON dd.card  = pc.card
LEFT JOIN cashback_dates cbd ON cbd.card = pc.card
LEFT JOIN setup_dates    sd  ON sd.card  = pc.card