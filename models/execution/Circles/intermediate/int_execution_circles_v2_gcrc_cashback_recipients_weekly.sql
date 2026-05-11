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
-- Cashback wallet sends gCRC (the ERC-20 wrapper of the gCRC group avatar)
-- to active app users; the Dune circles-v2-kpis dashboard counts an address
-- as having earned cashback in a week if it received >= 1 gCRC from the
-- cashback wallet that week. This intermediate exposes (week, address, amount)
-- with the 1 gCRC threshold applied at the aggregation level.
--
-- NB: this is a different cashback program from the gPay cashback already
-- modelled in fct_execution_gpay_cashback_*. The two are not unioned.
--
-- Source: int_execution_circles_v2_wrapper_transfers — gCRC was deployed
-- via the ERC20Lift mechanism so its transfers land here alongside the
-- personal-CRC wrappers. Reading the wrapper-transfers table avoids the
-- 10 GiB-blowing raw-logs scan.

{% set cashback_wallet = var('circles_v2_cashback_wallet') %}
{% set gcrc_token      = var('circles_v2_gcrc_token') %}

SELECT
    toStartOfWeek(block_timestamp, 1)                  AS week,
    to_address                                         AS address,
    sum(toFloat64(amount_raw) / pow(10, 18))           AS amount
FROM {{ ref('int_execution_circles_v2_wrapper_transfers') }}
WHERE token_address = '{{ gcrc_token }}'
  AND from_address  = '{{ cashback_wallet }}'
  AND block_timestamp < today()
GROUP BY week, to_address
HAVING amount >= 1
