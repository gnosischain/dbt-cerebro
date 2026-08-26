
    
    

select
    id as unique_field,
    count(*) as n_records

from `dbt`.`api_governance_proposals_live`
where id is not null
group by id
having count(*) > 1


