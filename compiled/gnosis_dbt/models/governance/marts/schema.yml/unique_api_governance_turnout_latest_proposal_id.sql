
    
    

select
    proposal_id as unique_field,
    count(*) as n_records

from `dbt`.`api_governance_turnout_latest`
where proposal_id is not null
group by proposal_id
having count(*) > 1


