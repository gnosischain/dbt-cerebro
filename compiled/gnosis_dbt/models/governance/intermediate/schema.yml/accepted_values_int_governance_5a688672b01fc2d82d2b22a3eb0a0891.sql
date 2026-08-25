
    
    

with all_values as (

    select
        power_source as value_field,
        count(*) as n_records

    from `dbt`.`int_governance_vote_power_source`
    group by power_source

)

select *
from all_values
where value_field not in (
    'Staked GNO (GBC)','GNO holdings','Delegated'
)


