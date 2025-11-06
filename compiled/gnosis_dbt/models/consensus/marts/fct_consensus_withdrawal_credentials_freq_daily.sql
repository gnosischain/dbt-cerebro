
SELECT 
    date
    ,CASE 
        WHEN cnt < 10 THEN '[1-10['
        WHEN cnt >= 10 AND cnt < 50 THEN '[10-50['
        WHEN cnt >= 50 AND cnt < 100 THEN '[50-100['
        WHEN cnt >= 100 AND cnt < 500 THEN '[100-500['
        WHEN cnt >= 500 AND cnt < 1000 THEN '[500-1000['
        WHEN cnt >= 1000 AND cnt < 2000 THEN '[1000-2000['
        ELSE '2000+'
    END AS bin_number_validators
    ,count() AS withdrawal_credentials_freq_cnt
FROM `dbt`.`int_consensus_withdrawal_credentials_daily`
GROUP BY 1, 2