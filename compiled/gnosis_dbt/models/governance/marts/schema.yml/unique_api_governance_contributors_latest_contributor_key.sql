
    
    

select
    contributor_key as unique_field,
    count(*) as n_records

from `dbt`.`api_governance_contributors_latest`
where contributor_key is not null
group by contributor_key
having count(*) > 1


