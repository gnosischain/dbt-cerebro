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
    tags=['production','execution','gnosis_app','gpay'],
    pre_hook=["SET join_algorithm = 'grace_hash'"],
    post_hook=["SET join_algorithm = 'default'"]
  )
}}
{% set enabled_topic  = 'ecdf3a3effea5783a3c4c2140e677577666428d44ed9d474a0b3a4c9943f8440' %}
{% set disabled_topic = 'aab4fa2b463f581b2b32cb3b7e3b704b9ce37cc209b5fb4d77e593ace4054276' %}

WITH

all_delay_modules AS (
    SELECT
        lower(replaceAll(sm.module_proxy_address, '0x', '')) AS delay_module_lc,
        sm.module_proxy_address                              AS delay_module,
        sm.gp_safe                                           AS pay_wallet
    FROM {{ ref('int_execution_gpay_safe_modules') }} sm
    WHERE sm.contract_type = 'DelayModule'
),

-- Delay modules touched in the incremental window. The macro bounds
-- l.block_timestamp by max(last_event_at) from {{ this }}; on full-refresh
-- / first run the macro emits nothing and we scan all history.
changed_delay_modules AS (
    SELECT DISTINCT lower(l.address) AS delay_module_lc
    FROM {{ source('execution','logs') }} l
    WHERE l.topic0 IN ('{{ enabled_topic }}', '{{ disabled_topic }}')
      AND lower(l.address) IN (SELECT delay_module_lc FROM all_delay_modules)
    {% if start_month and end_month %}
      AND l.block_timestamp >= toDateTime('{{ start_month }}')
      AND l.block_timestamp <  addMonths(toDateTime('{{ end_month }}'), 1)
    {% else %}
      {{ apply_monthly_incremental_filter('l.block_timestamp', 'last_event_at', add_and=true, lookback_days=2) }}
    {% endif %}
),

delay_modules AS (
    SELECT dm.*
    FROM all_delay_modules dm
    INNER JOIN changed_delay_modules cdm USING (delay_module_lc)
),

ga_users AS (
    SELECT address FROM {{ ref('int_execution_gnosis_app_users_current') }}
),

-- Full event history for ONLY the changed delay modules. Net-sum requires
-- full history for correctness, but the IN-scan is bounded to a small set.
module_events AS (
    SELECT
        concat('0x', lower(l.address))                          AS delay_module,
        concat('0x', lower(substring(l.data, 25, 40)))          AS module_address,
        if(l.topic0 = '{{ enabled_topic }}', 1, -1)             AS diff,
        l.block_timestamp                                       AS block_timestamp
    FROM {{ source('execution', 'logs') }} l
    WHERE l.topic0 IN ('{{ enabled_topic }}', '{{ disabled_topic }}')
      AND l.block_timestamp >= toDateTime('2023-06-01')
      AND lower(l.address) IN (SELECT delay_module_lc FROM delay_modules)
),

net_modules AS (
    SELECT
        delay_module,
        module_address,
        sum(diff)                                               AS net_enabled,
        minIf(block_timestamp, diff = 1)                        AS first_enabled_at,
        max(block_timestamp)                                    AS last_event_at
    FROM module_events
    WHERE module_address != '0x0000000000000000000000000000000000000000'
    GROUP BY delay_module, module_address
),

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

total_current AS (
    SELECT
        dm.pay_wallet                                           AS pay_wallet,
        countIf(n.net_enabled > 0)                              AS n_total_owners_current,
        max(n.last_event_at)                                    AS last_event_at
    FROM net_modules n
    INNER JOIN delay_modules dm ON dm.delay_module = n.delay_module
    GROUP BY dm.pay_wallet
),

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
    coalesce(o.onboarding_class, 'imported')                    AS onboarding_class,
    tc.last_event_at                                            AS last_event_at
FROM per_wallet p
LEFT JOIN total_current tc    ON tc.pay_wallet = p.pay_wallet
LEFT JOIN onboarding_class o  ON o.pay_wallet  = p.pay_wallet
