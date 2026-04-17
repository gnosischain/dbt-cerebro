{% set start_month = var('start_month', none) %}
{% set end_month   = var('end_month',   none) %}

{{
  config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    engine='ReplacingMergeTree()',
    order_by='(pay_wallet)',
    unique_key='pay_wallet',
    settings={'allow_nullable_key': 1},
    pre_hook=["SET join_algorithm = 'grace_hash'"],
    tags=['production','execution','gnosis_app','gpay']
  )
}}

WITH

-- Watermark: start of the current incremental batch. Used to scope the
-- "changed" scan. Everything before this is assumed stable.
-- On full-refresh / first run we push it back to 2023-06-01 (all time).
changed_window_start AS (
    SELECT
        {% if start_month and end_month %}
            toDateTime('{{ start_month }}')                    AS ts
        {% elif is_incremental() %}
            greatest(
                toDateTime('2023-06-01'),
                subtractDays(
                    coalesce(
                        (SELECT max(coalesce(first_ga_owner_at,
                                             initial_event_at,
                                             toDateTime('2023-06-01')))
                         FROM {{ this }}),
                        toDateTime('2023-06-01')
                    ),
                    30
                )                                               AS ts
        {% else %}
            toDateTime('2023-06-01')                           AS ts
        {% endif %}
),

gp_safes AS (
    SELECT address AS pay_wallet
    FROM {{ ref('int_execution_gpay_wallets') }}
),

ga_users AS (
    SELECT address FROM {{ ref('int_execution_gnosis_app_users_current') }}
),

-- All delay modules (delay_module proxy → GP Safe map).
-- IMPORTANT: `delay_module_lc` is built WITHOUT the 0x prefix so it can
-- match `execution.logs.address` (which is stored as raw hex, no 0x).
-- `delay_module` keeps the 0x prefix for downstream joins against CTEs
-- that also prefix via concat('0x', ...).
all_delay_modules AS (
    SELECT
        lower(replaceAll(sm.module_proxy_address, '0x', '')) AS delay_module_lc,
        sm.module_proxy_address                              AS delay_module,
        sm.gp_safe                                           AS pay_wallet
    FROM {{ ref('int_execution_gpay_safe_modules') }} sm
    WHERE sm.contract_type = 'DelayModule'
),

-- Delay modules that had ANY EnabledModule/DisabledModule event since the
-- watermark. On full-refresh / first run, this equals all delay modules.
changed_delay_modules AS (
    SELECT DISTINCT lower(l.address) AS delay_module_lc
    FROM {{ source('execution','logs') }} l
    WHERE l.topic0 IN (
            'ecdf3a3effea5783a3c4c2140e677577666428d44ed9d474a0b3a4c9943f8440',
            'aab4fa2b463f581b2b32cb3b7e3b704b9ce37cc209b5fb4d77e593ace4054276'
          )
      AND l.block_timestamp >= (SELECT ts FROM changed_window_start)
      AND lower(l.address) IN (SELECT delay_module_lc FROM all_delay_modules)
),

-- Restrict downstream to the delay modules that need recomputing.
delay_modules AS (
    SELECT dm.*
    FROM all_delay_modules dm
    INNER JOIN changed_delay_modules cdm
        ON cdm.delay_module_lc = dm.delay_module_lc
),

-- Full event history (back to 2023-06-01) for ONLY the changed delay
-- modules. Net-sum needs full history for correctness, but the IN-scan
-- is now bounded to a small number of delay modules.
module_events AS (
    SELECT
        concat('0x', lower(l.address))                          AS delay_module,
        concat('0x', lower(substring(l.data, 25, 40)))          AS module_address,
        if(l.topic0 = 'ecdf3a3effea5783a3c4c2140e677577666428d44ed9d474a0b3a4c9943f8440', 1, -1) AS diff,
        l.block_timestamp                                       AS block_timestamp
    FROM {{ source('execution', 'logs') }} l
    WHERE l.topic0 IN (
            'ecdf3a3effea5783a3c4c2140e677577666428d44ed9d474a0b3a4c9943f8440',
            'aab4fa2b463f581b2b32cb3b7e3b704b9ce37cc209b5fb4d77e593ace4054276'
          )
      AND l.block_timestamp >= toDateTime('2023-06-01')
      AND lower(l.address) IN (SELECT delay_module_lc FROM delay_modules)
),

-- Net enable state per (delay_module, enabled_module). Positive = currently enabled.
net_modules AS (
    SELECT
        delay_module,
        module_address,
        sum(diff)                                               AS net_enabled,
        minIf(block_timestamp, diff = 1)                        AS first_enabled_at
    FROM module_events
    WHERE module_address != '0x0000000000000000000000000000000000000000'
    GROUP BY delay_module, module_address
),

-- GA users attached to a delay module, joined back to the GP Safe.
ga_enabled AS (
    SELECT
        dm.pay_wallet                                           AS pay_wallet,
        n.module_address                                        AS ga_owner,
        n.net_enabled                                           AS net_enabled,
        n.first_enabled_at                                      AS first_enabled_at
    FROM net_modules n
    INNER JOIN delay_modules dm ON dm.delay_module = n.delay_module
    INNER JOIN ga_users       u ON u.address      = n.module_address
),

per_wallet AS (
    SELECT
        pay_wallet,
        min(first_enabled_at)                                   AS first_ga_owner_at,
        argMin(ga_owner, first_enabled_at)                      AS first_ga_owner_address,
        countIf(net_enabled > 0)                                AS n_ga_owners_current,
        countDistinct(ga_owner)                                 AS n_ga_owners_ever
    FROM ga_enabled
    GROUP BY pay_wallet
),

-- Total current modules enabled on the Delay (for n_total_owners_current).
-- Any module with net_enabled > 0 counts (GA or otherwise).
total_current AS (
    SELECT
        dm.pay_wallet                                           AS pay_wallet,
        countIf(n.net_enabled > 0)                              AS n_total_owners_current
    FROM net_modules n
    INNER JOIN delay_modules dm ON dm.delay_module = n.delay_module
    GROUP BY dm.pay_wallet
),

-- Onboarding class: iff the FIRST-ever module enabled on the Delay is a GA user.
first_enable_per_delay AS (
    SELECT
        delay_module,
        argMin(module_address, block_timestamp)                 AS first_enabled_module,
        min(block_timestamp)                                    AS first_enabled_at
    FROM module_events
    WHERE diff = 1
      AND module_address != '0x0000000000000000000000000000000000000000'
    GROUP BY delay_module
),

onboarding_class AS (
    SELECT
        dm.pay_wallet                                           AS pay_wallet,
        f.first_enabled_module                                  AS initial_owner_address,
        f.first_enabled_at                                      AS initial_event_at,
        if(f.first_enabled_module IN (SELECT address FROM ga_users),
           'onboarded_via_ga',
           'imported')                                          AS onboarding_class
    FROM first_enable_per_delay f
    INNER JOIN delay_modules dm ON dm.delay_module = f.delay_module
)

SELECT
    p.pay_wallet                                                AS pay_wallet,
    p.first_ga_owner_at                                         AS first_ga_owner_at,
    p.first_ga_owner_address                                    AS first_ga_owner_address,
    o.initial_event_at                                          AS initial_event_at,
    o.initial_owner_address                                     AS initial_owner_address,
    p.n_ga_owners_current > 0                                   AS is_currently_ga_owned,
    p.n_ga_owners_current                                       AS n_ga_owners_current,
    coalesce(tc.n_total_owners_current, 0)                      AS n_total_owners_current,
    coalesce(o.onboarding_class, 'imported')                    AS onboarding_class
FROM per_wallet p
LEFT JOIN total_current tc    ON tc.pay_wallet = p.pay_wallet
LEFT JOIN onboarding_class o  ON o.pay_wallet  = p.pay_wallet
