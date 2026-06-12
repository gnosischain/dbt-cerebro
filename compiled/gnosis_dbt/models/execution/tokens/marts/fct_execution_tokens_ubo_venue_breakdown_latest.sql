

WITH

latest_date AS (
    SELECT max(date) AS d
    FROM `dbt`.`int_execution_tokens_balances_daily`
    WHERE date < today() AND balance > 0
),

total_supply AS (
    SELECT
        token_address,
        any(symbol)      AS symbol,
        any(token_class) AS token_class,
        sum(balance)     AS total_balance,
        sum(balance_usd) AS total_balance_usd
    FROM `dbt`.`int_execution_tokens_balances_daily`
    WHERE date = (SELECT d FROM latest_date)
      AND balance > 0
      AND lower(address) != '0x0000000000000000000000000000000000000000'
    GROUP BY token_address
),

protocol_supply AS (
    SELECT
        token_address,
        protocol,
        sum(balance)     AS proto_balance,
        sum(balance_usd) AS proto_balance_usd
    FROM `dbt`.`fct_ubo_supply_claims_resolved_daily`
    WHERE date = (SELECT d FROM latest_date)
      AND balance > 0
    GROUP BY token_address, protocol
),

protocol_totals AS (
    SELECT
        token_address,
        sum(proto_balance)     AS total_protocol_balance,
        sum(proto_balance_usd) AS total_protocol_balance_usd
    FROM protocol_supply
    GROUP BY token_address
),

protocol_rows AS (
    SELECT
        p.token_address,
        t.symbol,
        t.token_class,
        p.protocol                                                    AS venue,
        p.proto_balance                                               AS balance,
        p.proto_balance_usd                                           AS balance_usd,
        round(p.proto_balance / nullIf(t.total_balance, 0) * 100, 2) AS percentage
    FROM protocol_supply p
    JOIN total_supply t ON t.token_address = p.token_address
    WHERE t.total_balance > 0
),

direct_rows AS (
    SELECT
        t.token_address,
        t.symbol,
        t.token_class,
        'direct'                                                      AS venue,
        greatest(t.total_balance     - coalesce(pt.total_protocol_balance,     0), 0) AS balance,
        greatest(t.total_balance_usd - coalesce(pt.total_protocol_balance_usd, 0), 0) AS balance_usd,
        round(
            greatest(t.total_balance - coalesce(pt.total_protocol_balance, 0), 0)
            / nullIf(t.total_balance, 0) * 100
        , 2)                                                          AS percentage
    FROM total_supply t
    LEFT JOIN protocol_totals pt ON pt.token_address = t.token_address
    WHERE t.total_balance > 0
)

SELECT token_address, symbol, token_class, venue, balance, balance_usd, percentage
FROM protocol_rows
UNION ALL
SELECT token_address, symbol, token_class, venue, balance, balance_usd, percentage
FROM direct_rows
ORDER BY token_address, balance_usd DESC