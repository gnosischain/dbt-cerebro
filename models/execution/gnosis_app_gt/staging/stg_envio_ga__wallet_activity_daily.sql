{{ config(
    materialized='table',
    engine='ReplacingMergeTree()',
    order_by='(activity_date, address)',
    settings={'allow_nullable_key': 1},
    tags=['production', 'execution', 'gnosis_app_gt', 'stretch', 'internal_only', 'privacy:tier_internal'],
    meta={
        'expose_to_mcp': false,
        'privacy_tier': 'internal',
        'api': {'exclude_from_api': true},
        'grain': 'wallet_day',
        'guard': 'STRETCH — one full scan of envio_ga.transaction_action (209M, no pruning). Reduced to distinct (app-active wallet, day). The DAU/WAU/MAU + cohort-retention spine. Scoped to is_app_active identities so it is a genuine app-activity feed, not the whole-Circles 626k avatar set.'
    }
) }}

-- STRETCH: exactly ONE full scan of envio_ga.transaction_action, reduced to one
-- row per (app-active wallet, calendar-day) with an `is_app_tagged_day` flag. The
-- compact time-series spine every downstream active-wallet / retention metric
-- reads. timestamp is real on-chain time (unix seconds); timestamp>0 guards epoch.
--
-- is_app_tagged_day = the wallet did a DELIBERATE app-feature action that day: a
-- swap, an auto-topup config, or an app-fee/topup transfer (MetriFee/PayTopUp/
-- AutoTopup). MetriTransfer is EXCLUDED (generic P2P send that inflates the broad
-- DAU); MetriFee — charged ON Metri actions, only ~356k rows — captures Metri
-- app-usage days WITHOUT joining the 92.6M MetriTransfer set (the app_transfer_ids
-- set is ~383k). Broad activity (any on-chain action) is the base grain; the flag
-- lets downstream expose an app-usage series comparable to the heuristic.
WITH app_transfer_ids AS (
    SELECT id
    FROM {{ source('envio_ga', 'transfer') }}
    WHERE _deleted = 0 AND transfer_type IN ('MetriFee', 'PayTopUp', 'AutoTopup')
)
SELECT
    lower(avatar_id)          AS address,
    toDate(timestamp)         AS activity_date,
    max(swap_id != '' OR auto_topup_id != '' OR transfer_id IN (SELECT id FROM app_transfer_ids))
                              AS is_app_tagged_day
FROM {{ source('envio_ga', 'transaction_action') }}
WHERE _deleted = 0
  AND avatar_id != ''
  AND timestamp > 0
  AND lower(avatar_id) IN (
      SELECT address
      FROM {{ ref('int_execution_gnosis_app_gt_user_activity') }}
      WHERE is_app_active
  )
GROUP BY address, activity_date
