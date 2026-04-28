{{
  config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    engine='ReplacingMergeTree()',
    order_by='(date, wallet_address, position_type, symbol)',
    partition_by='toStartOfMonth(date)',
    unique_key='(date, wallet_address, position_type, protocol, symbol)',
    settings={ 'allow_nullable_key': 1 },
    tags=['production','execution','dao_treasury','holdings_daily']
  )
}}

WITH wallets AS (
    SELECT lower(address) AS address, label
    FROM {{ ref('dao_treasury_wallets') }}
),

token_holdings AS (
    SELECT
        b.date                  AS date,
        b.address               AS wallet_address,
        w.label                 AS wallet_label,
        b.symbol                AS symbol,
        b.token_class           AS token_class,
        'wallet'                AS position_type,
        ''                      AS protocol,
        b.balance               AS balance,
        b.balance_usd           AS balance_usd
    FROM {{ ref('int_execution_tokens_balances_daily') }} b
    INNER JOIN wallets w ON w.address = b.address
    WHERE b.date < today()
      AND b.balance > 0
      {{ apply_monthly_incremental_filter('b.date', 'date', add_and=True) }}
),

lending_holdings AS (
    SELECT
        l.date                  AS date,
        l.user_address          AS wallet_address,
        w.label                 AS wallet_label,
        l.symbol                AS symbol,
        'LENDING'               AS token_class,
        'lending'               AS position_type,
        l.protocol              AS protocol,
        l.balance               AS balance,
        l.balance_usd           AS balance_usd
    FROM {{ ref('int_execution_lending_aave_user_balances_daily') }} l
    INNER JOIN wallets w ON w.address = l.user_address
    WHERE l.date < today()
      AND l.balance > 0
      {{ apply_monthly_incremental_filter('l.date', 'date', add_and=True) }}
)

SELECT * FROM token_holdings
UNION ALL
SELECT * FROM lending_holdings
