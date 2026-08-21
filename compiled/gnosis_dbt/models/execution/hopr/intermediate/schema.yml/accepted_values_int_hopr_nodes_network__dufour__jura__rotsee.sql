
    
    

with all_values as (

    select
        network as value_field,
        count(*) as n_records

    from `dbt`.`int_hopr_nodes`
    group by network

)

select *
from all_values
where value_field not in (
    'dufour','jura','rotsee'
)


