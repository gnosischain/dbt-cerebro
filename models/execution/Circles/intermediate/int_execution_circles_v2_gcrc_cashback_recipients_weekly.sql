{{
  config(
    materialized='table',
    engine='ReplacingMergeTree()',
    order_by='(week, address)',
    settings={'allow_nullable_key': 1},
    tags=['production','execution','circles_v2','cashback','weekly']
  )
}}

-- Weekly recipients of the Circles v2 gCRC cashback program.
--
-- Cashback wallet sends gCRC (group CRC token) to active app users; the
-- Dune circles-v2-kpis dashboard counts an address as having earned cashback
-- in a week if it received >= 1 gCRC from the cashback wallet that week.
-- This intermediate exposes (week, address, amount) and applies the 1 gCRC
-- threshold at the aggregation level.
--
-- NB: this is a different cashback program from the gPay cashback already
-- modelled in fct_execution_gpay_cashback_*.  The two should not be unioned.
--
-- Materialised as a full-rebuild table — the underlying transfer volume is
-- modest (gCRC supply, one wallet) and downstream is non-incremental too.

{% set cashback_wallet = '0x' ~ var('circles_v2_cashback_wallet')[2:] %}
{% set gcrc_token      = '0x' ~ var('circles_v2_gcrc_token')[2:] %}

WITH erc20_transfers AS (
    SELECT
        block_timestamp,
        concat('0x', address)                                          AS token_address,
        concat('0x', substring(topic1, 27, 40))                        AS from_address,
        concat('0x', substring(topic2, 27, 40))                        AS to_address,
        reinterpretAsUInt256(reverse(unhex(substring(data, 3, 64))))   AS amount_raw
    FROM (
        {{ dedup_source(
              source_ref=source('execution', 'logs'),
              partition_by='block_number, transaction_index, log_index',
              columns='block_number, transaction_index, log_index, transaction_hash, address, topic1, topic2, data, block_timestamp',
              pre_filter="topic0 = 'ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef' AND block_timestamp < today() AND lower(concat('0x', address)) = '" ~ gcrc_token ~ "'"
        ) }}
    )
    WHERE concat('0x', substring(topic1, 27, 40)) = '{{ cashback_wallet }}'
)

SELECT
    toStartOfWeek(block_timestamp, 1)                  AS week,
    to_address                                         AS address,
    sum(toFloat64(amount_raw) / pow(10, 18))           AS amount
FROM erc20_transfers
WHERE block_timestamp < today()
GROUP BY week, to_address
HAVING amount >= 1
