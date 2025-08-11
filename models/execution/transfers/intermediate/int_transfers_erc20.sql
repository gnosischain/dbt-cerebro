WITH

source AS (
    SELECT
        block_number
        ,block_timestamp
        ,transaction_index
        ,log_index
        ,transaction_hash
        ,concat('0x', address) AS token_address
        ,concat('0x', substring(topic1,25,40)) AS "from"
        ,concat('0x', substring(topic2,25,40) ) AS "to"
        ,toString(
            reinterpretAsUInt256(
                reverse(unhex(data))
            )
        ) AS "value"
    FROM {{ source('execution','logs') }}
    WHERE
        topic0 = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
        {{ apply_monthly_incremental_filter('block_timestamp', 'block_timestamp', 'true') }}

)

SELECT * FROM source
