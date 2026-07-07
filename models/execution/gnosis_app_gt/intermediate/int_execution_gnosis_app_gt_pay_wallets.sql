{{ config(
    materialized='table',
    engine='ReplacingMergeTree()',
    order_by='(pay_wallet)',
    settings={'allow_nullable_key': 1},
    tags=['production', 'execution', 'gnosis_app_gt', 'internal_only', 'privacy:tier_internal'],
    meta={
        'expose_to_mcp': false,
        'privacy_tier': 'internal',
        'api': {'exclude_from_api': true},
        'grain': 'pay_wallet',
        'guard': 'MEMBERSHIP only. GA-ownership (ga_owned / onboarding_class) is GT-UNAVAILABLE (PAY-D02) and MUST stay on the execution.logs heuristic. is_gp_pay_wallet seeds from the heuristic 1,252-safe set — this model never shadows the heuristic ownership labels.'
    }
) }}

-- Guardian-module membership: current state is derived by NETTING Enabled vs
-- Disabled per (safe_address, module_address) — NOT a per-id argMax latest-state
-- mirror (DBT-D05). Recovery* labels are excluded from membership. The guardian
-- universe over-covers the 1,252 GP pay-wallets ~112x, so `is_gp_pay_wallet`
-- flags the heuristic-confirmed GP subset for reconciliation.
WITH net AS (
    SELECT
        safe_address,
        module_address,
        sumIf(1, label = 'Enabled') - sumIf(1, label = 'Disabled') AS net_enabled,
        max(event_at)                                              AS last_event_at
    FROM {{ ref('stg_envio_ga__guardian_module') }}
    WHERE label IN ('Enabled', 'Disabled')
    GROUP BY safe_address, module_address
),

active AS (
    SELECT safe_address, module_address, last_event_at
    FROM net
    WHERE net_enabled > 0
)

SELECT
    a.safe_address                                                 AS pay_wallet,
    count()                                                        AS n_active_modules,
    groupArray(a.module_address)                                   AS module_addresses,
    max(a.last_event_at)                                           AS last_module_event_at,
    a.safe_address IN (
        SELECT lower(pay_wallet) FROM {{ ref('int_execution_gnosis_app_gpay_wallets') }}
    )                                                              AS is_gp_pay_wallet
FROM active a
GROUP BY a.safe_address
