{{
  config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    engine='ReplacingMergeTree()',
    order_by='(safe_address, block_time, tx_hash, token_address, counterparty, action)',
    partition_by='toStartOfMonth(date)',
    settings={ 'allow_nullable_key': 1 },
    tags=['production','execution','celo','gpay','activity']
  )
}}

{% set settlement = '0xc07cd8c24fb384d5e2b60a3ef39751f5d4cb69e1' %}

-- Classification lives here, not upstream in Dune (mirrors Gnosis Chain's
-- int_execution_gpay_activity.sql) so fixes/additions (like Reversal below)
-- are a dbt edit + re-run, not a Dune query edit + re-backfill.
--
-- CASE ordering matters: sender-based branches (Payment/Withdrawal) must be
-- checked before receiver-based branches (Reversal/Top-up). A hypothetical
-- Safe-to-Safe transfer has sender AND receiver both in `wallets`; checking
-- receiver first would misclassify it as a Top-up to the receiving Safe
-- instead of a Withdrawal from the sending Safe (safe_address below always
-- resolves to sender when sender is a Safe, so action must agree with that).

WITH wallets AS (
    SELECT safe_address FROM {{ ref('int_celo_gpay_wallets') }}
),

classified AS (
    SELECT
        t.block_date,
        t.block_time,
        t.tx_hash,
        t.sender,
        t.receiver,
        t.token_symbol,
        t.token_address,
        t.amount,
        t.amount_usd,
        CASE
            WHEN t.sender IN (SELECT safe_address FROM wallets)
             AND t.receiver = '{{ settlement }}'
            THEN 'Payment'

            WHEN t.sender IN (SELECT safe_address FROM wallets)
            THEN 'Withdrawal'

            WHEN t.receiver IN (SELECT safe_address FROM wallets)
             AND t.sender = '{{ settlement }}'
            THEN 'Reversal'

            WHEN t.receiver IN (SELECT safe_address FROM wallets)
            THEN 'Top-up'
        END AS action,
        CASE
            WHEN t.sender IN (SELECT safe_address FROM wallets) THEN t.sender
            ELSE t.receiver
        END AS safe_address,
        CASE
            WHEN t.sender IN (SELECT safe_address FROM wallets) THEN 'out'
            ELSE 'in'
        END AS direction,
        CASE
            WHEN t.sender IN (SELECT safe_address FROM wallets) THEN t.receiver
            ELSE t.sender
        END AS counterparty
    -- FINAL: don't depend on OPTIMIZE having run in click-runner (already
    -- failed once here on permissions) — without it, a transfer caught in
    -- two overlapping daily 3-day windows could double-count before a
    -- merge collapses it.
    FROM {{ source('crawlers_data', 'celo_gpay_transfers') }} t
    FINAL
    -- insert_overwrite + this macro recomputes the WHOLE current month's
    -- partition every run (not just literally-new rows) — required so
    -- int_celo_gpay_wallets (always a full, up-to-date rebuild — see that
    -- model's header) can correct an earlier misclassification within the
    -- same month. wallets itself never needs this treatment: it stays a
    -- full table rebuild regardless of transfer volume, since its size is
    -- bounded by total card count, not transaction count.
    {{ apply_monthly_incremental_filter('t.block_date', 'date', false) }}
)

-- action IS NULL means neither sender nor receiver matched a wallet at
-- classification time. This is a real, observed race: crawlers_data.
-- celo_gpay_transfers and int_celo_gpay_wallets are each independently
-- derived from the same live, continuously-indexing Dune spine, not from
-- one consistent snapshot — a Safe can be recognized by the transfers
-- extraction moments before or after it lands in the wallets snapshot.
-- Dropping these rows (rather than guessing which side is the Safe) is
-- self-healing WITHIN the current month's reprocessing window (see
-- incremental_strategy above) — a Safe recognized a few seconds/minutes
-- late (the only case actually observed) is comfortably within that
-- window. A gap spanning a full calendar-month boundary would not
-- self-heal under insert_overwrite; this hasn't happened in practice and
-- is not expected to, since wallet recognition lag has only ever been
-- seconds, not weeks.
SELECT
    tx_hash,
    block_time,
    block_date AS date,
    safe_address,
    action,
    direction,
    token_symbol,
    token_address,
    counterparty,
    amount,
    amount_usd
FROM classified
WHERE action IS NOT NULL
ORDER BY safe_address, block_time
