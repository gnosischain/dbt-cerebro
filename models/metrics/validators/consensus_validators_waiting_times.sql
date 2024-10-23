{{ 
    config(
        materialized='table'
    ) 
}}

WITH 
final AS (
    SELECT
        toDate(activation_eligibility_time) AS day,
        toInt64(activation_time) - toInt64(activation_eligibility_time) AS entry_delay,
        toInt64(withdrawable_time) - toInt64(exit_time) AS exit_delay
    FROM {{ ref('consensus_validators_status') }}
)

SELECT day, COALESCE(toFloat64(min(entry_delay)),0)/3600 AS value, 'Min Entry' AS label FROM final GROUP BY 1
UNION ALL
SELECT day, COALESCE(toFloat64(max(entry_delay)),0)/3600 AS value, 'Max Entry' AS label FROM final GROUP BY 1
UNION ALL
SELECT day, COALESCE(toFloat64(median(entry_delay)),0)/3600 AS value, 'Median Entry' AS label FROM final GROUP BY 1
UNION ALL
SELECT day, COALESCE(toFloat64(avg(entry_delay)),0)/3600 AS value, 'Mean Entry' AS label FROM final GROUP BY 1
UNION ALL
SELECT day, COALESCE(toFloat64(min(exit_delay)),0)/3600 AS value, 'Min Exit' AS label FROM final GROUP BY 1
UNION ALL
SELECT day, COALESCE(toFloat64(max(exit_delay)),0)/3600 AS value, 'Max Exit' AS label FROM final GROUP BY 1
UNION ALL
SELECT day, COALESCE(toFloat64(median(exit_delay)),0)/3600 AS value, 'Median Exit' AS label FROM final GROUP BY 1
UNION ALL
SELECT day, COALESCE(toFloat64(avg(exit_delay)),0)/3600 AS value, 'Mean Exit' AS label FROM final GROUP BY 1