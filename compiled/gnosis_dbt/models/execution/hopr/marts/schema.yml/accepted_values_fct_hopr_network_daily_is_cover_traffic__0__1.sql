
    
    

with all_values as (

    select
        is_cover_traffic as value_field,
        count(*) as n_records

    from `dbt`.`fct_hopr_network_daily`
    group by is_cover_traffic

)

select *
from all_values
where value_field not in (
    '0','1'
)


