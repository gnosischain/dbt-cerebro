{{ 
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        engine='MergeTree()',
        order_by='(date, validator_index)',
        unique_key='(date, validator_index)',
        partition_by='toStartOfMonth(date)',
        tags=["production", "consensus", "validators_apy"]
    ) 
}}

{% set start_month = var('start_month', none) %}
{% set end_month = var('end_month', none) %}
{% set validator_index_start = var('validator_index_start', none) %}
{% set validator_index_end = var('validator_index_end', none) %}

WITH

deposists AS (
    SELECT 
        toStartOfDay(slot_timestamp) AS date
        ,pubkey
        ,SUM(amount) AS amount
    FROM {{ ref('stg_consensus__deposits') }}
    WHERE 
        slot_timestamp < toDate('2025-04-30')
        {% if var('start_month', none) and var('end_month', none) %}
        AND toStartOfMonth(slot_timestamp) >= toDate('{{ var("start_month") }}')
        AND toStartOfMonth(slot_timestamp) <= toDate('{{ var("end_month") }}')
        {% else %}
        {{ apply_monthly_incremental_filter('slot_timestamp', 'date', 'true') }}
        {% endif %}
    GROUP BY 1, 2
),

deposists_requests AS (
    SELECT
        toStartOfDay(slot_timestamp) AS date,
        JSONExtractString(deposit, 'pubkey') AS pubkey,
        SUM(toUInt64(JSONExtractString(deposit, 'amount'))) AS amount
    FROM {{ ref('stg_consensus__execution_requests') }}
    ARRAY JOIN JSONExtractArrayRaw(payload, 'deposits') AS deposit
    WHERE
        1=1
        {% if var('start_month', none) and var('end_month', none) %}
        AND toStartOfMonth(slot_timestamp) >= toDate('{{ var("start_month") }}')
        AND toStartOfMonth(slot_timestamp) <= toDate('{{ var("end_month") }}')
        {% else %}
        {{ apply_monthly_incremental_filter('slot_timestamp', 'date', 'true') }}
        {% endif %}
    GROUP BY 1, 2
),

withdrawals AS (
    SELECT 
        toStartOfDay(slot_timestamp) AS date 
        ,validator_index
        ,SUM(amount) AS amount
    FROM {{ ref('stg_consensus__withdrawals') }}
    WHERE
        1=1
        {% if var('start_month', none) and var('end_month', none) %}
        AND toStartOfMonth(slot_timestamp) >= toDate('{{ var("start_month") }}')
        AND toStartOfMonth(slot_timestamp) <= toDate('{{ var("end_month") }}')
        {% else %}
        {{ apply_monthly_incremental_filter('slot_timestamp', 'date', 'true') }}
        {% endif %}
    GROUP BY 1, 2
),

{% if is_incremental() %}
current_partition AS (
    SELECT 
        max(toStartOfMonth(date)) AS month
        ,max(date)  AS max_date
    FROM {{ this }}
    {% if validator_index_start is not none and validator_index_end is not none %}
    WHERE validator_index >= {{ validator_index_start }}
    AND validator_index < {{ validator_index_end }}
    {% endif %}
),
prev_balance AS (
    SELECT 
        t1.validator_index
        ,argMax(t1.balance, t1.date) AS balance
    FROM {{ this }} t1
    CROSS JOIN current_partition t2
    WHERE 
        toStartOfMonth(t1.date) = t2.month
        {% if validator_index_start is not none and validator_index_end is not none %}
        AND t1.validator_index >= {{ validator_index_start }}
        AND t1.validator_index < {{ validator_index_end }}
        {% else %}
        AND 
        t1.date < t2.max_date
        {% endif %}
    GROUP BY t1.validator_index
),
{% endif %}

validators AS (
    SELECT
        toStartOfDay(t1.slot_timestamp, 'UTC') AS date,
        t1.validator_index,
        t1.pubkey,
        t1.balance,
        COALESCE(
            lagInFrame(toNullable(t1.balance), 1, NULL) OVER (
                PARTITION BY t1.validator_index
                ORDER BY date
                ROWS BETWEEN 1 PRECEDING AND CURRENT ROW
            ),
            {% if is_incremental() %}
                t2.balance
            {% else %}
                t1.effective_balance
            {% endif %}
        ) AS prev_balance,
        t1.balance - prev_balance AS balance_diff
    FROM {{ ref('stg_consensus__validators') }} t1
    {% if is_incremental() %}
    LEFT JOIN prev_balance t2
    ON t2.validator_index = t1.validator_index
    {% endif %}
    WHERE
        t1.status LIKE 'active_%'
        {% if validator_index_start is not none and validator_index_end is not none %}
        AND t1.validator_index >= {{ validator_index_start }}
        AND t1.validator_index < {{ validator_index_end }}
        {% endif %}
        {% if var('start_month', none) and var('end_month', none) %}
        AND toStartOfMonth(t1.slot_timestamp) >= toDate('{{ var("start_month") }}')
        AND toStartOfMonth(t1.slot_timestamp) <= toDate('{{ var("end_month") }}')
        {% else %}
        {{ apply_monthly_incremental_filter('slot_timestamp', 'date', 'true') }}
        {% endif %}
)

SELECT 
    t1.date AS date
    ,t1.validator_index AS validator_index
    ,t1.balance AS balance
    ,t1.balance_diff AS balance_diff_original
    ,COALESCE(t2.amount,0)  AS deposited_amount
    ,COALESCE(t3.amount,0)  AS withdrawaled_amount
    ,COALESCE(t4.amount,0)  AS deposited_amount_request
    ,t1.balance_diff - COALESCE(t2.amount,0) - COALESCE(t4.amount,0) + COALESCE(t3.amount,0) AS eff_balance_diff
    ,COALESCE(eff_balance_diff/nullIf(t1.prev_balance, 0),0) AS rate
    ,ROUND((POWER((1+rate),365) - 1) * 100,2) AS apy
FROM validators t1
LEFT JOIN 
    deposists t2
    ON t2.date = t1.date
    AND t2.pubkey = t1.pubkey
LEFT JOIN 
    withdrawals t3
    ON t3.date = t1.date
    AND t3.validator_index = t1.validator_index
LEFT JOIN 
    deposists_requests t4
    ON t4.date = t1.date
    AND t4.pubkey = t1.pubkey