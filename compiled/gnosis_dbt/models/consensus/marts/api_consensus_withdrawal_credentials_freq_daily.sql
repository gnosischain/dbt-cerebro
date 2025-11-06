
SELECT 
    date
    ,bin_number_validators AS label
    ,withdrawal_credentials_freq_cnt AS value
FROM `dbt`.`fct_consensus_withdrawal_credentials_freq_daily`
ORDER BY 1, 2