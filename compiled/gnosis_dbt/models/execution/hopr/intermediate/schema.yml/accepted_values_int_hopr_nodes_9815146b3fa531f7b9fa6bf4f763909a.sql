
    
    

with all_values as (

    select
        geo_source as value_field,
        count(*) as n_records

    from `dbt`.`int_hopr_nodes`
    group by geo_source

)

select *
from all_values
where value_field not in (
    'ipinfo','unenriched','no_ipv4'
)


