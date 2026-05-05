{{
    config(
        materialized='view',
        tags=['production', 'quarterly_data', 'tier0', 'api:nodes_observed', 'granularity:quarterly']
    )
}}

SELECT
    toStartOfQuarter(date) AS quarter,
    argMax(total_observed, date) AS nodes_observed
FROM (
    SELECT date, sum(observed_nodes) AS total_observed
    FROM {{ ref('int_esg_node_classification') }}
    WHERE date < today()
    GROUP BY date
)
GROUP BY quarter
ORDER BY quarter
