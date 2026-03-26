{{
    config(
        materialized='view',
        tags=['production', 'execution', 'circles', 'balances']
    )
}}

WITH latest_snapshot AS (
    SELECT max(date) AS date
    FROM {{ ref('int_execution_circles_balances_daily') }}
),
balances AS (
    SELECT
        version,
        account,
        token_id,
        token_address,
        last_activity_ts,
        balance_raw AS total_balance,
        demurraged_balance_raw AS demurraged_total_balance
    FROM {{ ref('int_execution_circles_balances_daily') }}
    WHERE date = (SELECT date FROM latest_snapshot)
),
latest_avatars AS (
    SELECT * FROM {{ ref('fct_execution_circles_avatars_current') }}
),
latest_tokens AS (
    SELECT * FROM {{ ref('fct_execution_circles_tokens_current') }}
)

SELECT
    b.version,
    b.account,
    aa.avatar_type AS account_avatar_type,
    aa.name AS account_name,
    aa.cid_v0_digest AS account_cid_v0_digest,
    b.token_id,
    b.token_address,
    lt.token_type,
    lt.token_owner,
    ta.avatar_type AS token_owner_avatar_type,
    ta.name AS token_owner_name,
    b.last_activity_ts,
    b.total_balance,
    b.demurraged_total_balance
FROM balances b
LEFT JOIN latest_avatars aa
    ON b.account = aa.avatar
LEFT JOIN latest_tokens lt
    ON b.token_address = lt.token
LEFT JOIN latest_avatars ta
    ON lt.token_owner = ta.avatar
