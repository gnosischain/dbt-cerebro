
    
    

with all_values as (

    select
        network as value_field,
        count(*) as n_records

    from `dbt`.`api_hopr_channel_activity_daily`
    group by network

)

select *
from all_values
where value_field not in (
    'dufour','jura'
)


