

-- One row per invited human. Carries the mint-cadence signals downstream
-- needed by the redesigned five-stage funnel:
--   Invited → ≥2 days minted → ≥7 days minted → ≥14 days minted → Active Minter.
--
-- Why redesigned: every accepted invite auto-mints once at acceptance, so the
-- previous "minted at least once" stage was a flat 100% conversion. The new
-- signals skip the acceptance mint and track real cadence in 30 / 60-day
-- windows post-invite.
--
-- Columns:
--   first_mint_at           - first mint event timestamp (typically = acceptance).
--   second_mint_date        - first calendar day with a mint that is NOT
--                             the invited_at date (the "did they come back?"
--                             signal).
--   days_to_second_mint     - dateDiff between invited_at date and second_mint_date.
--   n_mint_days_first_30d   - distinct mint days in [invited_at, invited_at + 30).
--   n_mint_days_first_60d   - distinct mint days in [invited_at, invited_at + 60).
--   became_active_minter_at - earliest date the avatar had mint_days_14dw = 14
--                             AND mint_14dw >= 0.8 * 336 (canonical Active
--                             Minter definition). NULL if never reached.




WITH invitees AS (
    SELECT
        avatar,
        invited_by                            AS inviter,
        block_timestamp                       AS invited_at
    FROM `dbt`.`int_execution_circles_v2_avatars`
    WHERE avatar_type = 'Human'
      AND invited_by IS NOT NULL
      AND invited_by != '0x0000000000000000000000000000000000000000'
      AND block_timestamp < today()
      
        
  

      
),

-- Per-avatar mint events for the avatars in this batch. Restricting by the
-- invitees subquery prevents reading the full mint history on incremental runs.
mint_events AS (
    -- Personal mints only — the invite-funnel "first mint" milestone is
    -- the invitee's own personalMint claim, not a group mint dropped on
    -- them or a migration backfill.
    SELECT
        to_address              AS avatar,
        block_timestamp         AS mint_at,
        toDate(block_timestamp) AS mint_date
    FROM `dbt`.`int_execution_circles_v2_mint_events`
    WHERE mint_kind = 'personal'
      AND to_address IN (SELECT avatar FROM invitees)
),

per_avatar AS (
    SELECT
        i.avatar                                                AS avatar,
        i.inviter                                               AS inviter,
        i.invited_at                                            AS invited_at,
        toDate(i.invited_at)                                    AS invited_date,
        min(m.mint_at)                                          AS first_mint_at,
        -- first mint *day* different from the invite day
        minIf(m.mint_date,
              m.mint_date != toDate(i.invited_at))              AS second_mint_date,
        uniqExactIf(
            m.mint_date,
            m.mint_date >= toDate(i.invited_at)
            AND m.mint_date <  toDate(i.invited_at) + 30
        )                                                       AS n_mint_days_first_30d,
        uniqExactIf(
            m.mint_date,
            m.mint_date >= toDate(i.invited_at)
            AND m.mint_date <  toDate(i.invited_at) + 60
        )                                                       AS n_mint_days_first_60d
    FROM invitees i
    LEFT JOIN mint_events m ON m.avatar = i.avatar
    GROUP BY i.avatar, i.inviter, i.invited_at
),

-- Earliest date each avatar in this batch hit Active Minter status. Joins
-- the 14-day rolling-window source of truth so we don't recompute coverage.
active_minter_first AS (
    SELECT
        avatar,
        min(date) AS became_active_minter_at
    FROM `dbt`.`int_execution_circles_v2_mint_activity_daily`
    WHERE mint_days_14dw = 14
      AND mint_14dw >= 0.8 * 336
      AND avatar IN (SELECT avatar FROM invitees)
    GROUP BY avatar
)

SELECT
    p.avatar                                                              AS avatar,
    p.inviter                                                             AS inviter,
    p.invited_at                                                          AS invited_at,
    -- min(DateTime) over an empty group yields '1970-01-01 00:00:00' under
    -- ClickHouse's default-value semantics. Map that sentinel back to NULL.
    nullIf(p.first_mint_at,    toDateTime('1970-01-01 00:00:00'))         AS first_mint_at,
    nullIf(p.second_mint_date, toDate('1970-01-01'))                      AS second_mint_date,
    if(nullIf(p.second_mint_date, toDate('1970-01-01')) IS NULL,
       NULL,
       dateDiff('day', p.invited_date, p.second_mint_date))               AS days_to_second_mint,
    toUInt16(p.n_mint_days_first_30d)                                     AS n_mint_days_first_30d,
    toUInt16(p.n_mint_days_first_60d)                                     AS n_mint_days_first_60d,
    nullIf(am.became_active_minter_at, toDate('1970-01-01'))              AS became_active_minter_at
FROM per_avatar p
LEFT JOIN active_minter_first am ON am.avatar = p.avatar