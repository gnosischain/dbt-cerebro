

WITH

blocks_clients AS (
    SELECT
        toStartOfDay(block_timestamp) AS date
        ,
arrayFilter(
    x -> x != '',
    /* split on every “non word-ish” character (dash, @, space, etc.) */
    splitByRegexp(
        '[^A-Za-z0-9\\.]+',            -- ⇽ anything that isn’t a–z, 0–9 or “.”
        arrayStringConcat(
            arrayMap(
                i -> if(
                    reinterpretAsUInt8(substring(unhex(extra_data), i, 1)) BETWEEN 32 AND 126,
                    reinterpretAsString(substring(unhex(extra_data), i, 1)),
                    ' '
                ),
                range(1, length(unhex(extra_data)) + 1)
            ),
            ''
        )
    )
)
 AS decoded_extra_data
        ,COUNT(*) AS cnt
    FROM `dbt`.`execution_blocks_production`
    
  
    
      
    

    WHERE 
    toStartOfMonth(toStartOfDay(block_timestamp)) >= (
      SELECT
        max(toStartOfMonth(date))
      FROM `dbt`.`execution_blocks_clients_version_daily`
    )
  

    GROUP BY 1, 2
)

SELECT
    date
    , multiIf(
        lower(decoded_extra_data[1]) = 'choose' 
         OR lower(decoded_extra_data[1]) = 'mysticryuujin'  
         OR lower(decoded_extra_data[1]) = 'sanae.io'
         OR decoded_extra_data[1] = ''  , 'Unknown',
        decoded_extra_data[1]
    )   AS client
    ,IF(length(decoded_extra_data)>1, 
        IF(decoded_extra_data[2]='Ethereum',decoded_extra_data[3],decoded_extra_data[2]), 
        ''
    ) AS version
    ,SUM(cnt) AS value
FROM blocks_clients
GROUP BY 1, 2, 3