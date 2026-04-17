{{
  config(
    materialized='table',
    engine='ReplacingMergeTree()',
    order_by='(user_pseudonym, gp_safe)',
    settings={ 'allow_nullable_key': 1 },
    tags=['production','execution','gpay','mixpanel']
  )
}}

WITH gp_safes AS (
    SELECT lower(address) AS gp_safe FROM {{ ref('int_execution_gpay_wallets') }}
),

initial_owners AS (
    SELECT
        oe.safe_address                            AS gp_safe,
        'initial_owner'                            AS identity_role,
        {{ pseudonymize_address('oe.owner') }}     AS user_pseudonym
    FROM {{ ref('int_execution_safes_owner_events') }} oe
    INNER JOIN gp_safes gs ON lower(oe.safe_address) = gs.gp_safe
    WHERE oe.event_kind = 'safe_setup'
      AND oe.owner IS NOT NULL
),

delegates AS (
    SELECT
        d.gp_safe                                          AS gp_safe,
        'delegate'                                         AS identity_role,
        {{ pseudonymize_address('d.delegate_address') }}   AS user_pseudonym
    FROM {{ ref('int_execution_gpay_spender_delegates_current') }} d
),

safe_self AS (
    -- gp_safes.gp_safe already comes from `lower(int_execution_gpay_wallets.address)`,
    -- which is lowercase 0x-prefixed. No re-prefixing.
    SELECT
        gp_safe                                AS gp_safe,
        'safe_self'                            AS identity_role,
        {{ pseudonymize_address('gp_safe') }}  AS user_pseudonym
    FROM gp_safes
)

SELECT * FROM initial_owners
UNION ALL
SELECT * FROM delegates
UNION ALL
SELECT * FROM safe_self
