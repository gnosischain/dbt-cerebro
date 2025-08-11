SELECT
    date
    ,label
    ,fork
    ,cnt
FROM {{ ref('int_p2p_discv5_forks_daily') }}
