
    
    

select
    date as unique_field,
    count(*) as n_records

from `dbt`.`api_governance_electorate_monthly`
where date is not null
group by date
having count(*) > 1


