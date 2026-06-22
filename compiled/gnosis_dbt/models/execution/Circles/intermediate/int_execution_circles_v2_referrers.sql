

-- "Users who start referring" — one row per inviter with their first referral
-- event: the first time the address appears as invited_by on a new Human
-- avatar registration (on-chain truth for the Circles invitation economy).
--
-- first_referral_in_app: 1 if that first invitee's registration tx was routed
-- through an active Gnosis App relayer (the invitee registered in-app).
-- first_inviter_fee_at: the stricter "paid referral" milestone — first time
-- the inviter actually received >= some CRC as an invitation fee (NULL if no
-- fee observed; fee-less invites still count as referrals).

WITH registrations AS (
    SELECT
        invited_by                              AS inviter,
        avatar                                  AS invitee,
        block_timestamp,
        transaction_hash
    FROM `dbt`.`int_execution_circles_v2_avatars`
    WHERE avatar_type = 'Human'
      AND invited_by IS NOT NULL
      AND invited_by != '0x0000000000000000000000000000000000000000'
      AND block_timestamp < today()
),

gnosis_app_txs AS (
    SELECT transaction_hash
    FROM `execution`.`transactions` tx
    WHERE tx.to_address = '0000000071727de22e5e9d8baf0edac6f37da032'
      AND lower(tx.from_address) IN (
          SELECT lower(replaceAll(address, '0x', ''))
          FROM `dbt`.`gnosis_app_relayers`
          WHERE is_active = 1
      )
      AND tx.block_timestamp >= toDateTime('2025-11-12')
      AND tx.block_timestamp < today()
),

first_fees AS (
    SELECT
        inviter,
        min(block_timestamp) AS first_inviter_fee_at
    FROM `dbt`.`int_execution_circles_v2_inviter_fees`
    GROUP BY inviter
)

SELECT
    r.inviter                                                       AS inviter,
    min(r.block_timestamp)                                          AS first_referral_at,
    toStartOfWeek(min(r.block_timestamp), 1)                        AS first_referral_week,
    count()                                                         AS n_referrals,
    argMin(toUInt8(g.transaction_hash IS NOT NULL AND g.transaction_hash != ''), r.block_timestamp) AS first_referral_in_app,
    any(f.first_inviter_fee_at)                                     AS first_inviter_fee_at
FROM registrations r
LEFT JOIN gnosis_app_txs g
    ON concat('0x', g.transaction_hash) = r.transaction_hash
LEFT JOIN first_fees f
    ON f.inviter = r.inviter
GROUP BY r.inviter