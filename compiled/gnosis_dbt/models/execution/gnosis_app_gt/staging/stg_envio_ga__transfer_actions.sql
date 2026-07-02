

-- STRETCH: exactly ONE full scan of envio_ga.transfer (108M rows; id-sorted, so
-- neither transfer_type nor _synced_block prunes — EXPLAIN ESTIMATE reads all
-- 108M). Emits the app / Circles-in-app action signals as a compact
-- per-(participant, transfer_type) count, consumed by
-- int_execution_gnosis_app_gt_user_activity.
--
-- The "participant" is the human ACTOR for each action type: the payer/sender
-- (`from`) for fees/sends/top-ups, the recipient (`to`) for mints/invites. A
-- single scan unpivots from/to via arrayJoin (no double read). Raw
-- MetriTransfer.`from` is mostly a Safe address (~6% identity-mapped) — that is
-- harmless because the activity model keeps only participants that resolve to a
-- registry/avatar identity.
--
-- DIRECTION GUARD (verified): PersonalMint.`from` is the zero address on 100% of
-- rows — the minter is only on `.to`, so PersonalMint MUST stay in the to-side
-- list below. Never move it to the from-side (that would drop all ~19.4k minters
-- and inject 0x000...0). Likewise HubTransfer/Erc20WrapperTransfer are counted on
-- both sides, PrimaryGroupFee/MetriFee on `from`, InvitationFee on `to`.
SELECT
    participant,
    transfer_type,
    count() AS n_events
FROM (
    SELECT
        transfer_type,
        arrayJoin(
            arrayConcat(
                -- from-side actor (payer / sender / funder)
                if(`from` != '' AND transfer_type IN (
                       'MetriTransfer', 'MetriFee', 'PayTopUp', 'AutoTopup',
                       'HubTransfer', 'PrimaryGroupFee', 'Erc20WrapperTransfer'),
                   [lower(`from`)], []),
                -- to-side actor (minter / invitee / top-up recipient)
                if(`to` != '' AND transfer_type IN (
                       'PersonalMint', 'InvitationFee', 'HubTransfer',
                       'Erc20WrapperTransfer', 'PayTopUp', 'AutoTopup'),
                   [lower(`to`)], [])
            )
        ) AS participant
    FROM `envio_ga`.`transfer`
    WHERE _deleted = 0
      AND transfer_type IN (
          'MetriTransfer', 'MetriFee', 'PayTopUp', 'AutoTopup', 'HubTransfer',
          'PersonalMint', 'PrimaryGroupFee', 'Erc20WrapperTransfer', 'InvitationFee'
      )
)
GROUP BY participant, transfer_type