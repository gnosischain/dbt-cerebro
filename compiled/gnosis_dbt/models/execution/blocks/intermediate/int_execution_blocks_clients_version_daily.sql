



WITH

deduped_blocks AS (
    SELECT
        block_timestamp,
        
arrayFilter(
    x -> x != '',
    /* split on every “non word-ish” character (dash, @, space, etc.) */
    splitByRegexp(
        '[^A-Za-z0-9\\.]+',            -- ⇽ anything that isn’t a–z, 0–9 or “.”
        arrayStringConcat(
            arrayMap(
                i -> if(
                    reinterpretAsUInt8(substring(unhex(coalesce(extra_data, '')), i, 1)) BETWEEN 32 AND 126,
                    reinterpretAsString(substring(unhex(coalesce(extra_data, '')), i, 1)),
                    ' '
                ),
                range(1, length(unhex(coalesce(extra_data, ''))) + 1)
            ),
            ''
        )
    )
)
 AS decoded_extra_data
    FROM (
        

SELECT block_timestamp, extra_data
FROM (
    SELECT
        block_timestamp, extra_data,
        ROW_NUMBER() OVER (
            PARTITION BY block_number
            ORDER BY insert_version DESC
        ) AS _dedup_rn
    FROM `execution`.`blocks`
    
    WHERE 
    block_timestamp > '1970-01-01'
    
  
    
    

   AND 
    toStartOfMonth(toDate(block_timestamp)) >= (
      SELECT toStartOfMonth(addDays(max(toDate(x1.date)), -0))
      FROM `dbt`.`int_execution_blocks_clients_version_daily` AS x1
      WHERE 1=1 
    )
    AND toDate(block_timestamp) >= (
      SELECT addDays(max(toDate(x2.date)), -0)
      FROM `dbt`.`int_execution_blocks_clients_version_daily` AS x2
      WHERE 1=1 
    )
  


    
)
WHERE _dedup_rn = 1

    )
),

clients_version AS (
    SELECT
        toStartOfDay(block_timestamp) AS date
        ,multiIf(
             lower(decoded_extra_data[1]) = 'choose'
            OR lower(decoded_extra_data[1]) = 'mysticryuujin'
            OR lower(decoded_extra_data[1]) = 'sanae.io'
            OR decoded_extra_data[1] = ''  ,
            'Unknown',
            decoded_extra_data[1]
        )   AS client
        ,IF(length(decoded_extra_data)>1,
            IF(decoded_extra_data[2]='Ethereum',decoded_extra_data[3],decoded_extra_data[2]),
            ''
        ) AS version
        ,COUNT(*) AS cnt
    FROM deduped_blocks
    GROUP BY 1, 2, 3
)

SELECT
    *
FROM clients_version