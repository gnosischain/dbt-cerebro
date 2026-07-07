{{ config(
    materialized='table',
    engine='ReplacingMergeTree()',
    order_by='(address)',
    settings={'allow_nullable_key': 1},
    pre_hook=["SET join_use_nulls = 1"],
    post_hook=["SET join_use_nulls = 0"],
    tags=['production', 'execution', 'gnosis_app_gt', 'internal_only', 'privacy:tier_internal'],
    meta={
        'expose_to_mcp': false,
        'privacy_tier': 'internal',
        'api': {'exclude_from_api': true},
        'grain': 'registered_identity',
        'guard': 'dim_circles_identity — the REGISTERED Circles-identity universe (~301k, ecosystem-wide incl Metri; 53% dormant). NEVER a user count. The CANONICAL GT active-user metric is is_engaged (= is_registered_active from int_..._user_activity, ~26.6k app-tagged); the active-user spine is the avatar table.'
    }
) }}

-- Ground-truth registered-identity dimension (= dim_circles_identity).
-- Registry (gnosis_app_user) LEFT JOIN avatar (spine) LEFT JOIN profile, plus
-- per-source engagement flags. `is_engaged` (~26.6k) is now sourced from the
-- CANONICAL activity model int_..._user_activity (registered + any app-tagged
-- action, incl. MetriFee/MetriTransfer), replacing the old ad-hoc 6-signal calc.
-- app_generation: Metri = legacy Gnosis App. app_scope on swaps separates the
-- current Gnosis App (gnosis_app) from Metri (legacy).
SELECT
    d.*,
    coalesce(act.is_registered_active, false)                     AS is_engaged,
    coalesce(act.is_app_active, false)                            AS is_app_active,
    coalesce(act.app_generation, 'none')                          AS app_generation,
    act.last_action_at                                            AS last_action_at
FROM (
    SELECT
        u.address                                                     AS address,
        u.created_at_block,
        u.lifetime_cashback_atoms,
        u.lifetime_cashback_atoms > 0                                 AS has_lifetime_cashback,
        coalesce(a.avatar_address, '') != ''                          AS has_circles_avatar,
        a.avatar_type                                                 AS circles_avatar_type,
        a.is_early_supporter                                          AS is_circles_early_supporter,
        a.verification_badge,
        nullIf(a.invited_by, '')                                      AS circles_invited_by,
        a.accepted_invite_at,
        a.created_at                                                  AS circles_avatar_created_at,
        a.earned_from_invites_crc,
        coalesce(p.address, '') != ''                                 AS has_profile,
        nullIf(p.profile_name, '')                                    AS profile_name,
        nullIf(p.profile_location, '')                                AS profile_location,
        multiIf(
            coalesce(a.avatar_address, '') = '', 'registry_only_no_avatar',
            coalesce(a.avatar_type, '')    = '', 'avatar_blank_type',
            a.avatar_type
        )                                                             AS user_segment,
        u.address IN (SELECT DISTINCT lower(address) FROM {{ ref('int_execution_gnosis_app_users_current') }}
                      WHERE address IS NOT NULL AND address != '')     AS is_heuristic_active,
        u.address IN (SELECT DISTINCT owner FROM {{ ref('stg_envio_ga__swaps') }} WHERE app_scope = 'gnosis_app') AS has_swapped_gnosis_app,
        u.address IN (SELECT DISTINCT owner FROM {{ ref('stg_envio_ga__swaps') }} WHERE app_scope = 'metri')      AS has_swapped_metri,
        u.address IN (SELECT DISTINCT owner FROM {{ ref('stg_envio_ga__cashbacks') }})            AS has_cashback,
        u.address IN (SELECT DISTINCT owner FROM {{ ref('stg_envio_ga__investment_accounts') }})  AS has_investment
    FROM {{ ref('stg_envio_ga__users') }} u
    LEFT JOIN {{ ref('stg_envio_ga__avatars') }}  a ON u.address = a.avatar_address
    LEFT JOIN {{ ref('stg_envio_ga__profiles') }} p ON u.address = p.address
) d
LEFT JOIN {{ ref('int_execution_gnosis_app_gt_user_activity') }} act ON d.address = act.address
