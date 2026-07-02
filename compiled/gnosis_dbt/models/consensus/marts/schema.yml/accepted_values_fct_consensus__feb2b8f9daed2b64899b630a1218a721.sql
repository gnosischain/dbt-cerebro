
    
    

with all_values as (

    select
        label as value_field,
        count(*) as n_records

    from `dbt`.`fct_consensus_graffiti_cloud`
    group by label

)

select *
from all_values
where value_field not in (
    '7D','30D','90D','All'
)


