
    
    

with all_values as (

    select
        channel_status as value_field,
        count(*) as n_records

    from `dbt`.`int_hopr_channels_events`
    group by channel_status

)

select *
from all_values
where value_field not in (
    'CLOSED','OPEN','PENDING_TO_CLOSE','unknown','unexpected'
)


