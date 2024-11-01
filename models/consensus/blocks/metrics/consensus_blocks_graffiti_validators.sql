{{ config(materialized='table') }}

WITH 

-- Get the withdrawal time for validators
withdrawable_validators AS (
    SELECT
        f_index AS proposer_index,
        {{ compute_timestamp_at_epoch('f_withdrawable_epoch') }} AS withdrawable_time     
    FROM {{ get_postgres('gnosis_chaind', 't_validators') }}
    WHERE f_withdrawable_epoch IS NOT NULL
),

-- Retrieve classified graffiti per proposer per day
daily_graffiti AS (
    SELECT
        day,
        f_proposer_index,
        CASE 
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
            WHEN POSITION('stakewise' IN LOWER(graffiti)) > 0 THEN 'StakeWise'
            ELSE graffiti
        END AS graffiti  
    FROM    
        {{ ref('consensus_blocks_graffiti') }}
),

-- Assign row numbers to identify sequences of the same graffiti
graffiti_changes AS (
    SELECT
        f_proposer_index,
        day,
        graffiti,
        ROW_NUMBER() OVER (PARTITION BY f_proposer_index ORDER BY day) AS rn1,
        ROW_NUMBER() OVER (PARTITION BY f_proposer_index, graffiti ORDER BY day) AS rn2
    FROM daily_graffiti
),

-- Calculate group identifiers based on row number differences
graffiti_periods AS (
    SELECT
        f_proposer_index,
        graffiti,
        (rn1 - rn2) AS grp,
        MIN(day) AS start_day,
        MAX(day) AS end_day
    FROM graffiti_changes
    GROUP BY f_proposer_index, graffiti, grp
),

-- Assign proposers to their graffiti on each day within their graffiti periods
proposer_graffiti_on_day AS (
    SELECT
        gp.f_proposer_index,
        day,
        gp.graffiti
    FROM graffiti_periods gp
    ARRAY JOIN
        arrayMap(i -> toDate(gp.start_day + i), range(toUInt32(gp.end_day - gp.start_day) + 1)) AS day
),

-- Exclude withdrawable proposers on and after their withdrawal date
active_proposers AS (
    SELECT
        pgod.f_proposer_index,
        pgod.day,
        pgod.graffiti
    FROM proposer_graffiti_on_day pgod
    LEFT JOIN withdrawable_validators wv ON pgod.f_proposer_index = wv.proposer_index
    WHERE wv.withdrawable_time IS NULL OR wv.withdrawable_time > pgod.day
),

-- Count unique active proposers per day and graffiti
cumulative_counts AS (
    SELECT
        day,
        graffiti AS graffiti_type,
        COUNT(DISTINCT f_proposer_index) AS cnt
    FROM active_proposers
    GROUP BY day, graffiti
),

-- Rank graffiti types by proposer count per day
ranked_graffiti AS (
    SELECT 
        day,
        graffiti_type,
        cnt,
        ROW_NUMBER() OVER (PARTITION BY day ORDER BY cnt DESC) AS r_cnt
    FROM cumulative_counts
)

-- Final aggregation and labeling of top 10 graffiti types
SELECT 
    *
FROM (
    SELECT 
        day,
        CASE
            WHEN r_cnt <= 10 THEN graffiti_type
            ELSE 'Others'
        END AS graffiti_top10,
        SUM(cnt) AS cnt
    FROM ranked_graffiti
    GROUP BY 1, 2
    ORDER BY day, graffiti_top10
)
WHERE cnt>0
