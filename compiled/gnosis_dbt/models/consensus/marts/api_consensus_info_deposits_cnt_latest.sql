

SELECT
    value
    ,change_pct
FROM 
    `dbt`.`fct_consensus_info_latest`
WHERE
    label = 'deposits_cnt'