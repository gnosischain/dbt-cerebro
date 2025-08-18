WITH

deposits_withdrawls_latest AS (
    SELECT
        label
        ,cnt
        ,total_amount
    FROM 
        `dbt`.`fct_consensus_deposits_withdrawls_daily`
    WHERE
        date = (SELECT MAX(date) FROM `dbt`.`fct_consensus_deposits_withdrawls_daily`)
),

deposits_withdrawls_7d AS (
    SELECT
        label
        ,cnt
        ,total_amount
    FROM 
        `dbt`.`fct_consensus_deposits_withdrawls_daily`
    WHERE
        date = subtractDays((SELECT MAX(date) FROM `dbt`.`int_p2p_discv4_clients_daily`), 7)
),

apy_latest AS (
    SELECT
        apy_7dma
    FROM 
        `dbt`.`fct_consensus_validators_apy_daily`
    WHERE
        date = (SELECT MAX(date) FROM `dbt`.`fct_consensus_validators_apy_daily`)
),

apy_7d AS (
    SELECT
       apy_7dma
    FROM 
        `dbt`.`fct_consensus_validators_apy_daily`
    WHERE
        date = subtractDays((SELECT MAX(date) FROM `dbt`.`fct_consensus_validators_apy_daily`), 7)
),

status_latest AS (
    SELECT
        status AS label
        ,CAST(COALESCE(cnt,0) AS Float64) AS value
    FROM 
        `dbt`.`int_consensus_validators_status_daily`
    WHERE
        date = (SELECT MAX(date) FROM `dbt`.`int_consensus_validators_status_daily`)
),

status_7d AS (
    SELECT
        status AS label
        ,CAST(COALESCE(cnt,0) AS Float64) AS value
    FROM 
        `dbt`.`int_consensus_validators_status_daily`
    WHERE
        date = subtractDays((SELECT MAX(date) FROM `dbt`.`int_consensus_validators_status_daily`), 7)
),



info_latest AS ( 
    SELECT
        'deposits_cnt' AS label
    ,COALESCE( (SELECT CAST(cnt AS Float64) FROM deposits_withdrawls_latest WHERE label = 'deposits'), 0) AS value
    UNION ALL
    SELECT
        'withdrawls_cnt' AS label
    ,COALESCE( (SELECT CAST(cnt AS Float64) FROM deposits_withdrawls_latest WHERE label = 'withdrawls'), 0) AS value
    UNION ALL
    SELECT
        'deposits_total_amount' AS label 
    ,COALESCE( (SELECT ROUND(total_amount,2) FROM deposits_withdrawls_latest WHERE label = 'deposits'), 0) AS value
    UNION ALL
    SELECT
        'withdrawls_total_amount' AS label 
    ,COALESCE( (SELECT ROUND(total_amount,2) FROM deposits_withdrawls_latest WHERE label = 'withdrawls'), 0) AS value
    UNION ALL
    SELECT
        'withdrawls_total_amount' AS label 
    ,COALESCE( (SELECT ROUND(total_amount,2) FROM deposits_withdrawls_latest WHERE label = 'withdrawls'), 0) AS value
    UNION ALL
    SELECT
        'APY7D' AS label 
    ,COALESCE((SELECT ROUND(apy_7dma,2) FROM apy_latest),0) AS value
    UNION ALL
    SELECT
       label 
    ,  value
    FROM status_latest
),

info_7d AS ( 
    SELECT
        'deposits_cnt' AS label
    ,COALESCE( (SELECT CAST(cnt AS Float64) FROM deposits_withdrawls_7d WHERE label = 'deposits'), 0) AS value
    UNION ALL
    SELECT
        'withdrawls_cnt' AS label
    ,COALESCE( (SELECT CAST(cnt AS Float64) FROM deposits_withdrawls_7d WHERE label = 'withdrawls'), 0) AS value
    UNION ALL
    SELECT
        'deposits_total_amount' AS label 
    ,COALESCE( (SELECT ROUND(total_amount,2) FROM deposits_withdrawls_7d WHERE label = 'deposits'), 0) AS value
    UNION ALL
    SELECT
        'APY7D' AS label 
    ,COALESCE((SELECT ROUND(apy_7dma,2) FROM apy_7d),0) AS value
    UNION ALL
    SELECT
       label 
    ,  value
    FROM status_7d
)

SELECT
    t1.label
    ,t1.value AS value
    ,IF(t1.value=0 AND t2.value=0, 0, ROUND((COALESCE(t2.value / NULLIF(t1.value, 0), 0) - 1) * 100, 1)) AS change_pct
FROM info_latest t1
INNER JOIN info_7d t2
ON t2.label = t1.label