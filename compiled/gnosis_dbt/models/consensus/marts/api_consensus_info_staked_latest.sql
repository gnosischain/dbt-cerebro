

SELECT
    toUInt32(value) AS value
    ,change_pct
FROM 
    `dbt`.`fct_consensus_info_latest`
WHERE
    label = 'Staked'