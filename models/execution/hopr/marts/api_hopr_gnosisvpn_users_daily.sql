{{
  config(
    materialized='view',
    tags=['production','hopr','gnosisvpn','tier3',
          'api:hopr_gnosisvpn_users','granularity:daily','window:30d']
  )
}}

/*
  Served GnosisVPN client activity, per network per day.

  TWO USER DEFINITIONS ARE EXPOSED AND THEY ARE NOT INTERCHANGEABLE:

    active_users_*  counts a client that redeemed a ticket OR first deployed in
                    the window. This mirrors HOPR's published definition and is
                    the one to quote against their figures.
    using_users_*   counts redemptions only. This is whether the product was
                    actually used.

  active minus using is the deployed-but-never-used cohort. A chart that plots
  both is honest; one that plots active while captioned "users of GnosisVPN" is
  not.

  The 30d window is 31 days inclusive and the 7d window is 7 -- an asymmetry
  inherited deliberately from HOPR's query so the numbers line up. See
  fct_hopr_gnosisvpn_users_daily for why it is reproduced rather than fixed.

  There is no bandwidth column. Tickets are probabilistic payments, not bytes.

  tier3, and GnosisVPN is in closed beta until Shangri-La (Sept 2026): treat
  every series here as early-access telemetry, not a settled product metric.
*/

SELECT
    date                    AS date,
    network                 AS network,

    active_users_30d        AS active_users_30d,
    active_users_7d         AS active_users_7d,
    active_users_1d         AS active_users_1d,

    using_users_30d         AS using_users_30d,
    using_users_7d          AS using_users_7d,
    using_users_1d          AS using_users_1d,

    new_clients             AS new_clients,
    tickets_redeemed        AS tickets_redeemed,
    redeemed_wxhopr         AS redeemed_wxhopr,
    channels_active         AS channels_active
FROM {{ ref('fct_hopr_gnosisvpn_users_daily') }}
ORDER BY network, date
