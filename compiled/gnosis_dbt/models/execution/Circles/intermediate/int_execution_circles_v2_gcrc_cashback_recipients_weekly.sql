

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




SELECT
    toStartOfWeek(block_timestamp, 1)                  AS week,
    to_address                                         AS address,
    sum(toFloat64(amount_raw) / pow(10, 18))           AS amount
FROM `dbt`.`int_execution_circles_v2_wrapper_transfers`
WHERE token_address = '0x548c20e6c24e4876e20dadbeab75362e2f5a4bc1'
  AND from_address  = '0x7abe74b71f2958b624cb2be0596678784c0caf6a'
  AND block_timestamp < today()
GROUP BY week, to_address
HAVING amount >= 1