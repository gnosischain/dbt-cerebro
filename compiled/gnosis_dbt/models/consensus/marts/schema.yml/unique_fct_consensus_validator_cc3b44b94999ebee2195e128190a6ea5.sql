
    
    

select
    withdrawal_credentials as unique_field,
    count(*) as n_records

from `dbt`.`fct_consensus_validators_explorer_latest`
where withdrawal_credentials is not null
group by withdrawal_credentials
having count(*) > 1


