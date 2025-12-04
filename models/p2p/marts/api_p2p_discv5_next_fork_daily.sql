{{ 
    config(
        materialized='view',
        tags=['production','p2p','discv4','forks', 'tier1', 'api: discv5_next_fork_d']
    ) 
}}


SELECT
    date
    ,fork
    ,cnt
FROM {{ ref('int_p2p_discv5_forks_daily') }}
WHERE label = 'Next Fork' 
ORDER BY date ASC, fork ASC
