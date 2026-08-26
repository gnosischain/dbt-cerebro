
    
    

with all_values as (

    select
        population as value_field,
        count(*) as n_records

    from `dbt`.`api_governance_concentration_latest`
    group by population

)

select *
from all_values
where value_field not in (
    'voters_by_vp','voters_by_votes','delegates_by_delegators'
)


