{{ 
   config(
       materialized='table',
   ) 
}}

{% set num_bins = 100 %}

WITH stats AS (
   SELECT 
       MIN(CAST(f_effective_balance AS FLOAT)/32000000000) as min_total,
       MAX(CAST(f_effective_balance AS FLOAT)/32000000000) as max_total,
       (MAX(CAST(f_effective_balance AS FLOAT)/32000000000) - MIN(CAST(f_effective_balance AS FLOAT)/32000000000))/{{num_bins}} as bin_size
   FROM {{ get_postgres('gnosis_chaind', 't_validators') }}
),
series AS (
   SELECT number + 1 as bucket 
   FROM numbers({{num_bins}})
),
validators_effective_balance AS (
   SELECT 
       width_bucket(
           CAST(f_effective_balance AS FLOAT)/32000000000,
           (SELECT min_total FROM stats),
           (SELECT max_total FROM stats) + 0.000001, -- Add small amount to include upper bound
           {{num_bins}}
       ) as bucket,
       COUNT(*) as cnt
   FROM {{ get_postgres('gnosis_chaind', 't_validators') }}
   GROUP BY 1
)

SELECT 
   s.bucket,
   CONCAT(
       ROUND(min_total + (s.bucket-1) * bin_size, 2),
       ' - ',
       ROUND(min_total + s.bucket * bin_size, 2)
   ) as range,
   COALESCE(v.cnt, 0) as cnt
FROM series s
CROSS JOIN stats
LEFT JOIN validators_effective_balance v ON s.bucket = v.bucket
ORDER BY s.bucket