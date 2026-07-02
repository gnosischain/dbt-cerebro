
    
    

select
    date as unique_field,
    count(*) as n_records

from `dbt`.`fct_consensus_validators_income_total_daily`
where date is not null
group by date
having count(*) > 1


