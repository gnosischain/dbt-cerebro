{{ 
    config(
        materialized='view',
        tags=['production','p2p', 'tier1', 'api:discv5_current_fork_count', 'granularity:daily']
    ) 
}}

SELECT
    date
    ,fork
    ,cnt
FROM {{ ref('int_p2p_discv5_forks_daily') }}
WHERE label = 'Current Fork' 
ORDER BY date ASC, fork ASC
