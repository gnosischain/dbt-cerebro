
    
    

select
    delegate as unique_field,
    count(*) as n_records

from `dbt`.`api_governance_delegates_latest`
where delegate is not null
group by delegate
having count(*) > 1


