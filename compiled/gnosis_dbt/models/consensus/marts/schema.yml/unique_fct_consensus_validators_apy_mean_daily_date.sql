
    
    

select
    date as unique_field,
    count(*) as n_records

from `dbt`.`fct_consensus_validators_apy_mean_daily`
where date is not null
group by date
having count(*) > 1


