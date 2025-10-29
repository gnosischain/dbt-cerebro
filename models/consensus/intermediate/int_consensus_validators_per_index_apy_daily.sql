{{ 
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        engine='MergeTree()',
        order_by='(date, validator_index)',
        unique_key='(date, validator_index)',
        partition_by='toStartOfMonth(date)',
        settings={ 'allow_nullable_key': 1 },
        tags=["production", "consensus", "validators_apy"]
    ) 
}}

{% set start_month = var('start_month', none) %}
{% set end_month = var('end_month', none) %}
{% set validator_index_start = var('validator_index_start', none) %}
{% set validator_index_end = var('validator_index_end', none) %}

WITH

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
        greatest(
            t1.balance - MOD( t1.balance, 32000000000)
            + toUInt64(roundBankers(MOD(t1.balance, 32000000000) / 32000000000) * 32000000000)
            , 32000000000) AS balance_mod,
        COALESCE(
            lagInFrame(toNullable(t1.balance), 1, NULL) OVER (
                PARTITION BY t1.validator_index
                ORDER BY date
                ROWS BETWEEN 1 PRECEDING AND CURRENT ROW
            ),
            {% if is_incremental() %}
                t2.balance
            {% else %}
                0--t1.effective_balance
            {% endif %}
        ) AS prev_balance,
        IF(prev_balance=0, 
            0, 
            greatest(
                prev_balance - MOD( prev_balance, 32000000000)
                + toUInt64(roundBankers(MOD(prev_balance, 32000000000) / 32000000000) * 32000000000)
                , 32000000000))  AS prev_balance_mod,
        t1.balance - prev_balance AS balance_diff,
        balance_mod - prev_balance_mod AS balance_mod_diff,
        t1.status AS status
    FROM {{ ref('stg_consensus__validators') }} t1
    {% if is_incremental() %}
    LEFT JOIN prev_balance t2
    ON t2.validator_index = t1.validator_index
    {% endif %}
    WHERE
        (t1.status LIKE 'active_%' OR t1.status = 'pending_queued')
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
    ,t1.status AS status
    ,t1.balance AS balance
    ,t1.balance_mod AS balance_mod
    ,t1.balance_diff AS balance_diff_original
    ,t1.balance_mod_diff AS deposited_amount
    ,COALESCE(t3.amount,0)  AS withdrawaled_amount
    ,balance_diff_original - deposited_amount + withdrawaled_amount AS eff_balance_diff
    ,eff_balance_diff/IF(t1.prev_balance=0, deposited_amount, toInt64(t1.prev_balance)) AS rate
    ,ROUND((POWER((1+rate),365) - 1) * 100,2) AS apy
FROM validators t1
LEFT JOIN 
    withdrawals t3
    ON t3.date = t1.date
    AND t3.validator_index = t1.validator_index