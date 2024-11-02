{{
   config(
       materialized='incremental',
       incremental_strategy='delete+insert',
       engine='ReplacingMergeTree()',
       order_by='(day, graffiti_top10)',
       unique_key='(day, graffiti_top10)',
       partition_by='partition_month',
       settings={
           "allow_nullable_key": 1
       }
   )
}}

WITH blocks_graffiti AS (
    SELECT
        partition_month
        ,day
        ,CASE 
            WHEN graffiti IS NULL 
                OR graffiti = '' 
                OR LENGTH(TRIM(graffiti)) = 0 
                OR graffiti = '\0'
                OR graffiti LIKE '\0%'
                OR char_length(graffiti) = 0
            THEN 'No Graffiti'
            WHEN POSITION('lighthouse' IN LOWER(graffiti)) > 0 THEN 'Lighthouse'
            WHEN POSITION('nimbus' IN LOWER(graffiti)) > 0 THEN 'Nimbus'
            WHEN POSITION('prysm' IN LOWER(graffiti)) > 0 THEN 'Prysm'
            WHEN POSITION('teku' IN LOWER(graffiti)) > 0 THEN 'Teku'
            WHEN POSITION('dappnode' IN LOWER(graffiti)) > 0 THEN 'DappNode'
            WHEN POSITION('stakewise' IN LOWER(graffiti)) > 0 THEN 'StateWise'
            ELSE graffiti
        END AS graffiti
        ,cnt
        ,ROW_NUMBER() OVER (PARTITION BY day ORDER BY cnt DESC) AS r_cnt
    FROM (
        SELECT
            partition_month
            ,day
            ,graffiti
            ,SUM(cnt) AS cnt
        FROM    
             {{ ref('consensus_blocks_graffiti') }}
        {% if is_incremental() %}
        WHERE partition_month >= (SELECT max(partition_month) FROM {{ this }})
        {% endif %}
        GROUP BY 1, 2, 3
    )
       
)

SELECT 
    partition_month
    ,day
    ,CASE
        WHEN r_cnt <= 10 THEN graffiti
        ELSE 'Others'
    END AS graffiti_top10
    ,SUM(cnt) AS cnt
FROM blocks_graffiti
GROUP BY 1, 2, 3