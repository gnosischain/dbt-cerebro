{{
  config(
    materialized='incremental',
    incremental_strategy=('append' if var('start_month', none) else 'delete+insert'),
    engine='ReplacingMergeTree()',
    order_by='(invited_at, avatar)',
    unique_key='(avatar)',
    partition_by='toStartOfMonth(invited_at)',
    settings={'allow_nullable_key': 1},
    tags=['production','execution','circles_v2','invite_funnel']
  )
}}

-- One row per invitee. Joins RegisterHuman to first PersonalMint to support
-- cohort conversion analysis (invited → minted → active minter).
--
--   invited_at        - block_timestamp of the RegisterHuman event
--   inviter           - the inviter address (humans only)
--   first_mint_at     - block_timestamp of the avatar's first mint event
--                       (NULL if never minted)
--   days_to_first_mint - dateDiff between the two (NULL if never minted)
--
-- Incremental keyed by `avatar`. Pre-existing rows in the table for the
-- current cohort already carry `first_mint_at`; we only refresh the trailing
-- month so a late-arriving first-mint backfills correctly.

{% set start_month = var('start_month', none) %}
{% set end_month   = var('end_month',   none) %}

WITH invitees AS (
    SELECT
        avatar,
        invited_by                            AS inviter,
        block_timestamp                       AS invited_at
    FROM {{ ref('int_execution_circles_v2_avatars') }}
    WHERE avatar_type = 'Human'
      AND invited_by IS NOT NULL
      AND invited_by != '0x0000000000000000000000000000000000000000'
      AND block_timestamp < today()
      {% if start_month and end_month %}
        AND toStartOfMonth(toDate(block_timestamp)) >= toDate('{{ start_month }}')
        AND toStartOfMonth(toDate(block_timestamp)) <= toDate('{{ end_month }}')
      {% else %}
        {{ apply_monthly_incremental_filter(
              source_field='block_timestamp',
              destination_field='invited_at',
              add_and=True) }}
      {% endif %}
),

first_mints AS (
    SELECT
        to_address                                AS avatar,
        min(block_timestamp)                      AS first_mint_at
    FROM {{ ref('int_execution_circles_v2_hub_transfers') }}
    WHERE from_address = '0x0000000000000000000000000000000000000000'
      AND to_address  != '0x0000000000000000000000000000000000000000'
    GROUP BY to_address
)

SELECT
    i.avatar                                                  AS avatar,
    i.inviter                                                 AS inviter,
    i.invited_at                                              AS invited_at,
    fm.first_mint_at                                          AS first_mint_at,
    if(fm.first_mint_at IS NULL,
       NULL,
       dateDiff('day', toDate(i.invited_at), toDate(fm.first_mint_at)))  AS days_to_first_mint
FROM invitees i
LEFT JOIN first_mints fm ON fm.avatar = i.avatar
