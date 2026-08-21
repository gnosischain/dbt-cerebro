
    
    

with all_values as (

    select
        network as value_field,
        count(*) as n_records

    from `dbt`.`contracts_hopr_registry`
    group by network

)

select *
from all_values
where value_field not in (
    'dufour','jura','rotsee'
)


