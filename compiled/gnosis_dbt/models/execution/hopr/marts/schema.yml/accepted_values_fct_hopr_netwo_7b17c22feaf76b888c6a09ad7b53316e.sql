
    
    

with all_values as (

    select
        network as value_field,
        count(*) as n_records

    from `dbt`.`fct_hopr_network_daily`
    group by network

)

select *
from all_values
where value_field not in (
    'dufour','jura','rotsee'
)


