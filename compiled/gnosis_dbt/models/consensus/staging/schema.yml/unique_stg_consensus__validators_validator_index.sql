
    
    

select
    validator_index as unique_field,
    count(*) as n_records

from `dbt`.`stg_consensus__validators`
where validator_index is not null
group by validator_index
having count(*) > 1


