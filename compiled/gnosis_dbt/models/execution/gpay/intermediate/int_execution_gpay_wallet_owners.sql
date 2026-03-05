



WITH gpay_wallets AS (
    SELECT address
    FROM `dbt`.`stg_gpay__wallets`
),

safe_setup_logs AS (
    SELECT
        lower(replaceAll(address, '0x', ''))  AS addr_raw,
        block_timestamp,
        replaceAll(data, '0x', '')            AS data_hex
    FROM `execution`.`logs`
    WHERE lower(replaceAll(topic0, '0x', ''))
          = '141df868a6331af528e38c83b7aa03edc19be66e37ae67f9285bf4f8e3c6a1a8'
      AND lower(replaceAll(address, '0x', ''))
          IN (SELECT lower(replaceAll(address, '0x', '')) FROM gpay_wallets)
      AND block_timestamp >= toDateTime('2023-06-01')
      
        AND block_timestamp > (SELECT coalesce(max(block_timestamp), toDateTime('1970-01-01')) FROM `dbt`.`int_execution_gpay_wallet_owners`)
      
)

SELECT
    concat('0x', addr_raw)                                                  AS pay_wallet,
    lower(concat('0x', substring(data_hex, 1 + 5*64 + 24, 40)))            AS owner,
    toUInt32(reinterpretAsUInt256(reverse(unhex(substring(data_hex, 1 + 1*64, 64))))) AS threshold,
    block_timestamp
FROM safe_setup_logs