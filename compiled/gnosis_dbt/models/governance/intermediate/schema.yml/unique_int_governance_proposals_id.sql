
    
    

select
    id as unique_field,
    count(*) as n_records

from `dbt`.`int_governance_proposals`
where id is not null
group by id
having count(*) > 1


