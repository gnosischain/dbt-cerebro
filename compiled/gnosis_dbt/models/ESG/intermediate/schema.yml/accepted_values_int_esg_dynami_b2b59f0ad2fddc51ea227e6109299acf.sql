
    
    

with all_values as (

    select
        carbon_intensity_source as value_field,
        count(*) as n_records

    from `dbt`.`int_esg_dynamic_power_consumption`
    group by carbon_intensity_source

)

select *
from all_values
where value_field not in (
    'country_specific','world_average'
)


